import socket
import threading
import time
import sys
import os

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
                print(f"[DATA] {stream.device_id}: {stream.value} (ts: {stream.timestamp})")
        except Exception:
            pass

# ── Loop principal ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Inicia a Thread que vai ficar ouvindo as respostas infinitamente
    listener_thread = threading.Thread(target=listen_for_responses, daemon=True)
    listener_thread.start()

    # Inicia a thread de dados
    data_thread = threading.Thread(target=listen_for_data, daemon=True)
    data_thread.start()

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
            