'''
import socket, sys
sys.path.insert(0, '.')
from proto import smart_city_pb2

cmd = smart_city_pb2.Command()
cmd.action = smart_city_pb2.Command.TURN_OFF  # ou TURN_ON, CHANGE_FREQ

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6004))   # porta TCP da câmera
s.sendall(cmd.SerializeToString())
s.close()
print("Comando enviado!")
'''

import socket, sys
sys.path.insert(0, '.')
from proto import smart_city_pb2

cmd = smart_city_pb2.Command()
cmd.action = smart_city_pb2.Command.CHANGE_FREQ
cmd.parameter = 1.0   # força o campo parameter a ser serializado
                       # isso faz o protobuf incluir o envelope todo

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6004))
s.sendall(cmd.SerializeToString())
s.close()
print(f"Bytes enviados: {cmd.SerializeToString().hex()}")