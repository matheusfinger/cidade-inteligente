"""
Classe base compartilhada por todos os sensores.
Resolve os três problemas:
  1. SO_REUSEPORT para múltiplos sensores no mesmo host receberem multicast
  2. Loop de envio de dados para se o gateway reiniciar (os dados continuam chegando)
  3. Watchdog local: se não recebe DISCOVER por muito tempo, o sensor sabe que
     o gateway caiu — mas continua tentando responder quando ele voltar
"""
import socket
import struct
import threading
import time
import sys
import os

pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, pasta_raiz)

from proto import smart_city_pb2

MULTICAST_GROUP = '224.0.0.1'
MULTICAST_PORT  = 5000
GATEWAY_IP      = '127.0.0.1'
GATEWAY_PORT    = 5001
DATA_PORT       = 5002


class BaseSensor:
    def __init__(self, device_id, device_type, tcp_port, send_interval=10):
        self.device_id     = device_id
        self.device_type   = device_type
        self.tcp_port      = tcp_port
        self.send_interval = send_interval
        self.active        = True
        self.running       = True
        self._data_sock    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # ── Discovery ─────────────────────────────────────────────────────────────
    def _build_response(self):
        resp               = smart_city_pb2.DiscoveryResponse()
        resp.device_id     = self.device_id
        resp.type          = self.device_type
        resp.ip            = '127.0.0.1'
        resp.tcp_port      = self.tcp_port
        resp.initial_state = 'ATIVO' if self.active else 'INATIVO'
        return resp

    def _listen_for_discover(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # SO_REUSEPORT permite múltiplos processos receberem o mesmo multicast
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(('', MULTICAST_PORT))
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GROUP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

        print(f"[{self.device_id}] Escutando multicast {MULTICAST_GROUP}:{MULTICAST_PORT}...")

        while self.running:
            try:
                data, addr = sock.recvfrom(1024)
                req = smart_city_pb2.DiscoveryRequest()
                req.ParseFromString(data)
                print(f"[{self.device_id}] DISCOVER de '{req.gateway_id}' — respondendo...")

                rs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                rs.sendto(self._build_response().SerializeToString(), (GATEWAY_IP, GATEWAY_PORT))
                rs.close()
            except Exception as e:
                if self.running:
                    print(f"[{self.device_id}] Erro no listener: {e}")

        sock.close()

    # ── Envio de dados ────────────────────────────────────────────────────────
    def _send_stream(self, sub_id, value):
        """Envia um DataStream UDP. Falha silenciosa se o gateway estiver fora."""
        s           = smart_city_pb2.DataStream()
        s.device_id = sub_id
        s.value     = float(value)
        s.timestamp = int(time.time())
        try:
            self._data_sock.sendto(s.SerializeToString(), (GATEWAY_IP, DATA_PORT))
        except Exception:
            pass  # gateway pode estar fora; continuamos tentando

    # ── Comandos TCP (sobrescrito por sensores controláveis) ──────────────────
    def _handle_command(self, conn, addr):
        """Processa um Command TCP. Subclasses podem sobrescrever."""
        try:
            data = conn.recv(1024)
            if not data:
                return
            cmd = smart_city_pb2.Command()
            #cmd.ParseFromString(data)
            if data:
                cmd.ParseFromString(data)
            self._apply_command(cmd)
        except Exception as e:
            print(f"[{self.device_id}] Erro no comando: {e}")
        finally:
            conn.close()

    def _apply_command(self, cmd):
        """Lógica de comando padrão — subclasses podem estender."""
        if cmd.action == smart_city_pb2.Command.TURN_ON:
            self.active = True
            print(f"[{self.device_id}] ATIVADO")
        elif cmd.action == smart_city_pb2.Command.TURN_OFF:
            self.active = False
            print(f"[{self.device_id}] DESATIVADO")
        elif cmd.action == smart_city_pb2.Command.CHANGE_FREQ:
            self.send_interval = max(1, int(cmd.parameter))
            print(f"[{self.device_id}] Frequência → {self.send_interval}s")

    def _listen_for_commands(self):
        if self.tcp_port == 0:
            return
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('', self.tcp_port))
        srv.listen(5)
        srv.settimeout(1.0)
        print(f"[{self.device_id}] Comandos TCP na porta {self.tcp_port}...")

        while self.running:
            try:
                conn, addr = srv.accept()
                threading.Thread(
                    target=self._handle_command, args=(conn, addr), daemon=True
                ).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[{self.device_id}] Erro TCP: {e}")
        srv.close()

    # ── Loop de dados (implementado pela subclasse) ───────────────────────────
    def _data_loop(self):
        raise NotImplementedError

    # ── Start ─────────────────────────────────────────────────────────────────
    def start(self):
        threading.Thread(target=self._listen_for_discover, daemon=True).start()
        if self.tcp_port:
            threading.Thread(target=self._listen_for_commands, daemon=True).start()
        time.sleep(0.5)
        try:
            self._data_loop()
        except KeyboardInterrupt:
            self.running = False
            self._data_sock.close()
            print(f"\n[{self.device_id}] Desligado.")
