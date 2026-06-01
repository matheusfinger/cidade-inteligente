import socket
import struct
import sys
import os
import time
import random

# Ajuste do path para achar a pasta proto
pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(pasta_raiz)

from proto import smart_city_pb2

MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT = 5000
GATEWAY_IP = '127.0.0.1'
GATEWAY_PORT = 5001
MY_TCP_PORT = 6001  # Porta TCP que este sensor usará no futuro para comandos

def listen_and_respond():
    # Configura o socket para ouvir o Multicast
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', MULTICAST_PORT))

    mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"[*] Sensor de Temperatura iniciado.")
    print(f"[*] Escutando Multicast {MULTICAST_GROUP}:{MULTICAST_PORT}...\n")

    while True:
        data, addr = sock.recvfrom(1024)
        request = smart_city_pb2.DiscoveryRequest()
        
        try:
            request.ParseFromString(data)
            print(f"[!] Requisição recebida do: {request.gateway_id}")
            
            # Monta a resposta estruturada
            response = smart_city_pb2.DiscoveryResponse()
            response.device_id = "Temp-Sensor-Centro"
            response.type = smart_city_pb2.TEMPERATURE
            response.ip = '127.0.0.1'
            response.tcp_port = MY_TCP_PORT
            response.initial_state = "ATIVO"

            # Envia resposta Unicast direto para a porta 5001 do Gateway
            resp_bytes = response.SerializeToString()
            send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            send_sock.sendto(resp_bytes, (GATEWAY_IP, GATEWAY_PORT))
            send_sock.close()
            
            print(f"[+] Credenciais enviadas para o Gateway!")
            
            # Quebra o loop só nesse teste inicial, já que o foco é o aperto de mão
            break
            
        except Exception as e:
            pass # Ignora lixo de rede que não seja do nosso Protobuf
    
    print("\n[*] Iniciando envio contínuo de dados...")
    data_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # A porta 5002 será dedicada no Gateway apenas para receber dados brutos
    DATA_PORT = 5002 
    
    while True:
        temperatura = round(random.uniform(25.0, 35.0), 2)
        
        # Monta a mensagem de dados
        stream = smart_city_pb2.DataStream()
        stream.device_id = "Temp-Sensor-Centro"
        stream.value = temperatura
        stream.timestamp = int(time.time())
        
        data_sock.sendto(stream.SerializeToString(), (GATEWAY_IP, DATA_PORT))
        print(f"[>] Enviado: {temperatura}°C")
        
        time.sleep(5) # Aguarda 5 segundos até a próxima leitura
    

if __name__ == "__main__":
    listen_and_respond()