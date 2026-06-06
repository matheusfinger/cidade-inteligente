import socket
import threading
import time
import sys
import os
import statistics

# Adiciona o diretório raiz do projeto ao sys.path para importar o módulo proto
pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(pasta_raiz)

from proto import smart_city_pb2

MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT = 5000
GATEWAY_IP = '127.0.0.1'  # IP local
GATEWAY_PORT = 5001       # Porta UDP dedicada para o Gateway ouvir os sensores
DATA_PORT = 5002
DISCOVERY_INTERVAL = 15   # segundos entre DISCOVERs
OFFLINE_TIMEOUT    = 45   # segundos sem resposta → marca OFFLINE

# Nossa "memória RAM" para guardar os dispositivos descobertos
dispositivos_ativos = {}
dispositivos_lock   = threading.Lock()

# Histórico para cálculos estatísticos (Analytics)
historico_dados = {}
historico_lock = threading.Lock()
MAX_HISTORICO = 100 # Mantém apenas as últimas 100 leituras para não estourar a RAM

def listen_for_responses():
    """
    Thread dedicada a ouvir respostas Unicast UDP dos sensores.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # erro
    sock.bind((GATEWAY_IP, GATEWAY_PORT))
    print(f"[Gateway] Escutando discovery em 0.0.0.0:{GATEWAY_PORT}...")

    while True:
        # Fica bloqueado aqui até receber dados de algum sensor
        data, addr = sock.recvfrom(1024) 
        
        response = smart_city_pb2.DiscoveryResponse()
        try:
            # Desserializa o binário de volta para o objeto Protobuf
            response.ParseFromString(data)
            
            # Armazena as informações estruturadas no dicionário
            with dispositivos_lock:
                ja_existia = response.device_id in dispositivos_ativos
                dispositivos_ativos[response.device_id] = {
                    "ip":        response.ip,
                    "tcp_port":  response.tcp_port,
                    "type":      response.type,
                    "state":     response.initial_state,
                    "last_seen": time.time(),
                }
            if ja_existia:
                print(f"[Gateway] Reconectado: {response.device_id}")
            else:
                print(f"[Gateway] Novo dispositivo: {response.device_id} | TCP: {response.tcp_port}")
        except Exception as e:
            print(f"[Gateway] Erro ao processar resposta de {addr}: {e}")

# ── Discovery: envia DISCOVER periódico ──────────────────────────────────────
def discovery_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    request            = smart_city_pb2.DiscoveryRequest()
    request.gateway_id = "Gateway-Central"
    msg                = request.SerializeToString()

    while True:
        sock.sendto(msg, (MULTICAST_GROUP, MULTICAST_PORT))
        print(f"[Gateway] DISCOVER enviado → {MULTICAST_GROUP}:{MULTICAST_PORT}")
        time.sleep(DISCOVERY_INTERVAL)

# ── Watchdog: remove dispositivos sem resposta ────────────────────────────────
def watchdog_loop():
    """Remove da memória qualquer dispositivo que não respondeu ao último DISCOVER."""
    while True:
        time.sleep(DISCOVERY_INTERVAL)
        agora = time.time()
        with dispositivos_lock:
            offline = [
                did for did, info in dispositivos_ativos.items()
                if agora - info.get("last_seen", 0) > OFFLINE_TIMEOUT
            ]
            for did in offline:
                del dispositivos_ativos[did]
                print(f"[Gateway] ⚠ Dispositivo removido (timeout): {did}")


def listen_for_data():
    """
    Thread dedicada a ouvir o fluxo contínuo de dados (DataStream) dos sensores.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # erro
    sock.bind((GATEWAY_IP, DATA_PORT))
    print(f"[Gateway] Escutando dados em 0.0.0.0:{DATA_PORT}...")

    while True:
        data, addr = sock.recvfrom(1024)
        stream = smart_city_pb2.DataStream()
        try:
            stream.ParseFromString(data)
            base_id = stream.device_id.split(':')[0]
            with dispositivos_lock:
                conhecido = (stream.device_id in dispositivos_ativos
                             or base_id in dispositivos_ativos)
                # atualiza last_seen para qualquer um dos dois IDs
                for key in [stream.device_id, base_id]:
                    if key in dispositivos_ativos:
                        dispositivos_ativos[key]["last_seen"] = time.time()
            if conhecido:
                # Salva no histórico para o Analytics
                with historico_lock:
                    if stream.device_id not in historico_dados:
                        historico_dados[stream.device_id] = []
                    
                    historico_dados[stream.device_id].append(stream.value)
                    
                    # Limita o tamanho da lista (janela deslizante)
                    if len(historico_dados[stream.device_id]) > MAX_HISTORICO:
                        historico_dados[stream.device_id].pop(0)

                print(f"[DATA] {stream.device_id}: {stream.value} (ts: {stream.timestamp})")
        except Exception:
            pass

GATEWAY_TCP_PORT = 7000 # Porta que o Cliente Analítico vai conectar

def forward_command_to_sensor(device_id, command_msg):
    """Abre uma conexão TCP com o sensor e repassa o comando."""
    with dispositivos_lock:
        if device_id not in dispositivos_ativos:
            return False, "Dispositivo não encontrado ou offline."
        
        info = dispositivos_ativos[device_id]
        if info["tcp_port"] == 0:
            return False, "Dispositivo não aceita comandos (porta TCP 0)."
            
        alvo_ip = info["ip"]
        alvo_porta = info["tcp_port"]

    try:
        # Gateway agindo como "Cliente TCP" do Sensor
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2.0)
        s.connect((alvo_ip, alvo_porta))
        s.sendall(command_msg.SerializeToString())
        s.close()
        return True, "Comando enviado com sucesso."
    except Exception as e:
        return False, f"Falha ao contatar sensor: {e}"

def handle_client(conn, addr):
    """Processa a requisição TCP de um Cliente Analítico."""
    try:
        data = conn.recv(4096)
        if not data: return
        
        req = smart_city_pb2.ClientRequest()
        req.ParseFromString(data)
        
        resp = smart_city_pb2.ClientResponse()
        resp.success = True
        
        # 1. STATUS: Retorna a lista de dispositivos
        if req.type == smart_city_pb2.ClientRequest.GET_STATUS:
            linhas = []
            with dispositivos_lock:
                for did, info in dispositivos_ativos.items():
                    linhas.append(f"- {did} | Tipo: {info['type']} | Estado: {info['state']} | TCP: {info['tcp_port']}")
            resp.message = "\n".join(linhas) if linhas else "Nenhum dispositivo ativo."

        # 2. ANALYTICS: Calcula métricas agregadas na RAM
        elif req.type == smart_city_pb2.ClientRequest.GET_ANALYTICS:
            did = req.target_device_id
            with historico_lock:
                valores = historico_dados.get(did, [])
            
            if not valores:
                resp.success = False
                resp.message = "Sem dados suficientes para este dispositivo."
            else:
                media = statistics.mean(valores)
                # Desvio padrão requer pelo menos 2 valores
                desvio = statistics.stdev(valores) if len(valores) > 1 else 0.0
                resp.message = f"Análise de {did} (Últimas {len(valores)} leituras):\nMédia: {media:.2f}\nDesvio Padrão: {desvio:.2f}"

        # 3. COMANDOS: Repassa a ordem para a Fonte de Dados
        elif req.type == smart_city_pb2.ClientRequest.SEND_COMMAND:
            sucesso, msg = forward_command_to_sensor(req.target_device_id, req.command_payload)
            resp.success = sucesso
            resp.message = msg

        # Devolve a resposta serializada para o Cliente
        conn.sendall(resp.SerializeToString())
        
    except Exception as e:
        print(f"[Gateway] Erro ao tratar cliente {addr}: {e}")
    finally:
        conn.close()

def listen_for_clients():
    """Servidor TCP principal do Gateway."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((GATEWAY_IP, GATEWAY_TCP_PORT))
    srv.listen(5)
    print(f"[Gateway] Servidor TCP Analítico escutando em {GATEWAY_IP}:{GATEWAY_TCP_PORT}...")

    while True:
        try:
            conn, addr = srv.accept()
            # Cria uma sub-thread para atender o cliente sem travar o Gateway
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
        except Exception as e:
            print(f"[Gateway] Erro no accept TCP: {e}")

# ── Loop principal ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Inicia a Thread que vai ficar ouvindo as respostas infinitamente
    listener_thread = threading.Thread(target=listen_for_responses, daemon=True)
    listener_thread.start()

    # Inicia a thread de dados
    data_thread = threading.Thread(target=listen_for_data, daemon=True)
    data_thread.start()

    tcp_thread = threading.Thread(target=listen_for_clients, daemon=True)
    tcp_thread.start()

    # Um pequeno delay de meio segundo apenas para garantir que a porta de escuta abriu
    time.sleep(0.5)

    # 2. Dispara a requisição Multicast em loop
    threading.Thread(target=discovery_loop, daemon=True).start()
    threading.Thread(target=watchdog_loop,  daemon=True).start()

    # 3. Loop principal para manter o programa vivo e exibir a memória
    
    try:
        while True:
            time.sleep(10) # A cada 10 segundos, mostra quem está salvo na RAM
            with dispositivos_lock:
                ids = list(dispositivos_ativos.keys())
            print(f"\n--- Memória do Gateway ({len(ids)} dispositivo(s)) ---")
            for did in ids:
                print(f"  {did}")
            print()
    except KeyboardInterrupt:
        print("\n[Gateway] Desligando...")
            