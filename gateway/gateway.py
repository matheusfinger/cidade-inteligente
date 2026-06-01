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

# Nossa "memória RAM" para guardar os dispositivos descobertos
dispositivos_ativos = {}

def listen_for_responses():
    """
    Thread dedicada a ouvir respostas Unicast UDP dos sensores.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((GATEWAY_IP, GATEWAY_PORT))
    print(f"[*] Gateway escutando respostas em {GATEWAY_IP}:{GATEWAY_PORT}...\n")

    while True:
        # Fica bloqueado aqui até receber dados de algum sensor
        data, addr = sock.recvfrom(1024) 
        
        response = smart_city_pb2.DiscoveryResponse()
        try:
            # Desserializa o binário de volta para o objeto Protobuf
            response.ParseFromString(data)
            
            # Armazena as informações estruturadas no dicionário
            dispositivos_ativos[response.device_id] = {
                "ip": response.ip,
                "tcp_port": response.tcp_port,
                "type": response.type,
                "state": response.initial_state
            }
            print(f"[+] Dispositivo registrado: {response.device_id} | Porta TCP: {response.tcp_port}")
            
        except Exception as e:
            print(f"[-] Erro ao processar pacote de {addr}: {e}")

def send_discovery_request():
    """
    Dispara o grito no Multicast.
    """
    request = smart_city_pb2.DiscoveryRequest()
    request.gateway_id = "Gateway-Central"
    message_bytes = request.SerializeToString()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)

    print(f"[*] Enviando Discovery Multicast para {MULTICAST_GROUP}:{MULTICAST_PORT}...")
    sock.sendto(message_bytes, (MULTICAST_GROUP, MULTICAST_PORT))
    sock.close()

DATA_PORT = 5002

def listen_for_data():
    """
    Thread dedicada a ouvir o fluxo contínuo de dados (DataStream) dos sensores.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((GATEWAY_IP, DATA_PORT))
    print(f"[*] Gateway escutando DADOS na porta {DATA_PORT}...")

    while True:
        data, addr = sock.recvfrom(1024)
        stream = smart_city_pb2.DataStream()
        
        try:
            stream.ParseFromString(data)
            
            # Aqui você vincula o dado recebido ao dispositivo que já estava na memória RAM
            if stream.device_id in dispositivos_ativos:
                print(f"[DATA] {stream.device_id}: {stream.value}°C (Timestamp: {stream.timestamp})")
                # Dica para depois: Podemos criar uma lista dentro do dicionário 
                # para armazenar o histórico e calcular a Média/Desvio Padrão depois!
                
        except Exception as e:
            pass

if __name__ == "__main__":
    # 1. Inicia a Thread que vai ficar ouvindo as respostas infinitamente
    listener_thread = threading.Thread(target=listen_for_responses, daemon=True)
    listener_thread.start()

    # Inicia a thread de dados
    data_thread = threading.Thread(target=listen_for_data, daemon=True)
    data_thread.start()

    # Um pequeno delay de meio segundo apenas para garantir que a porta de escuta abriu
    time.sleep(0.5)

    # 2. Dispara a requisição Multicast
    send_discovery_request()

    # 3. Loop principal para manter o programa vivo e exibir a memória
    try:
        while True:
            time.sleep(10) # A cada 10 segundos, mostra quem está salvo na RAM
            print(f"\n--- Memória do Gateway ---")
            print(f"Dispositivos ativos: {list(dispositivos_ativos.keys())}\n")
    except KeyboardInterrupt:
        print("\n[*] Desligando Gateway Inteligente...")