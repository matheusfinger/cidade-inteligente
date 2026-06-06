import socket
import sys
import os

# Ajuste do path para achar a pasta proto
pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pasta_raiz)

from proto import smart_city_pb2

GATEWAY_IP = '127.0.0.1'
GATEWAY_TCP_PORT = 7000

def send_request(req):
    """
    Abre o socket TCP, envia a requisição Protobuf e aguarda a resposta.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3.0)
        s.connect((GATEWAY_IP, GATEWAY_TCP_PORT))
        
        # Envia requisição
        s.sendall(req.SerializeToString())
        
        # Recebe resposta
        data = s.recv(4096)
        s.close()
        
        if not data:
            print("[-] Nenhuma resposta do Gateway.")
            return None
            
        resp = smart_city_pb2.ClientResponse()
        resp.ParseFromString(data)
        return resp
        
    except ConnectionRefusedError:
        print("\n[-] Erro: O Gateway parece estar offline (Conexão recusada).")
        return None
    except Exception as e:
        print(f"\n[-] Erro de comunicação: {e}")
        return None

def menu():
    """
    Loop principal com a interface de linha de comando.
    """
    while True:
        print("\n" + "="*45)
        print(" 🏙️  CLIENTE ANALÍTICO - SMART CITY")
        print("="*45)
        print("1. Listar Fontes de Dados Conectadas")
        print("2. Consultar Analytics (Média/Desvio Padrão)")
        print("3. Enviar Comando de Controle para Sensor")
        print("0. Sair")
        
        opcao = input("\nEscolha uma opção: ")

        if opcao == '0':
            print("Encerrando cliente...")
            break
            
        # --- OPÇÃO 1: STATUS DA REDE ---
        elif opcao == '1':
            req = smart_city_pb2.ClientRequest()
            req.type = smart_city_pb2.ClientRequest.GET_STATUS

            req.target_device_id = "todos"
            
            resp = send_request(req)
            if resp:
                print("\n--- STATUS DA REDE ---")
                print(resp.message)

        # --- OPÇÃO 2: CONSULTAS ESTATÍSTICAS ---
        elif opcao == '2':
            device_id = input("Digite o ID exato do dispositivo (ex: AirQuality-Industrial): ")
            req = smart_city_pb2.ClientRequest()
            req.type = smart_city_pb2.ClientRequest.GET_ANALYTICS
            req.target_device_id = device_id
            
            resp = send_request(req)
            if resp:
                print("\n--- RESULTADO ANALYTICS ---")
                print(resp.message)

        # --- OPÇÃO 3: CONTROLE REMOTO TCP ---
        elif opcao == '3':
            device_id = input("Digite o ID exato do dispositivo alvo: ")
            print("\nComandos disponíveis:")
            print("1. Ligar (TURN_ON)")
            print("2. Desligar (TURN_OFF)")
            print("3. Alterar Frequência de Envio (CHANGE_FREQ)")
            print("4. Alterar Limiar de Alerta (SET_THRESHOLD)")
            cmd_op = input("Escolha o comando: ")
            
            cmd = smart_city_pb2.Command()
            if cmd_op == '1':
                cmd.action = smart_city_pb2.Command.TURN_ON
            elif cmd_op == '2':
                cmd.action = smart_city_pb2.Command.TURN_OFF
            elif cmd_op == '3':
                cmd.action = smart_city_pb2.Command.CHANGE_FREQ
                cmd.parameter = float(input("Nova frequência (em segundos): "))
            elif cmd_op == '4':
                cmd.action = smart_city_pb2.Command.SET_THRESHOLD
                cmd.parameter = float(input("Novo limiar (valor numérico): "))
            else:
                print("Comando inválido.")
                continue

            req = smart_city_pb2.ClientRequest()
            req.type = smart_city_pb2.ClientRequest.SEND_COMMAND
            req.target_device_id = device_id
            
            # Protobuf exige o CopyFrom para anexar sub-mensagens
            req.command_payload.CopyFrom(cmd)
            
            resp = send_request(req)
            if resp:
                print("\n--- RESPOSTA DO COMANDO ---")
                status = "✅ SUCESSO" if resp.success else "❌ FALHA"
                print(f"{status}: {resp.message}")
        else:
            print("Opção inválida.")

if __name__ == '__main__':
    menu()