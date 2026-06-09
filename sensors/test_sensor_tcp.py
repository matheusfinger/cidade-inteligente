'''import socket, sys, os
sys.path.insert(0, '.')
from proto import smart_city_pb2

cmd = smart_city_pb2.Command()
cmd.action    = smart_city_pb2.Command.CHANGE_FREQ
cmd.parameter = 5.0   # muda para 5 segundos

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 6002))   # 6002 = semáforo, 6003 = qualidade do ar
s.sendall(cmd.SerializeToString())
s.close()
print("Comando enviado!")
'''
import socket, sys, os
sys.path.insert(0, '.')
from proto import smart_city_pb2

cmd = smart_city_pb2.Command()
cmd.action    = smart_city_pb2.Command.CHANGE_FREQ
cmd.parameter = 20.0   # muda para x segundos

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 63722)) 
s.sendall(cmd.SerializeToString())
s.close()
print("Comando enviado!")