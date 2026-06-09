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
        self.send_interval = send_interval
        self.active        = True
        self.running       = True
        self._data_sock    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # tcp_port=0 → SO escolhe; tcp_port>0 → porta fixa (legado/testes)
        # _tcp_port_real é preenchido após o bind, e é o que vai no DiscoveryResponse
        self._tcp_port_hint = tcp_port   # 0 = dinâmica, >0 = fixa
        self._tcp_port_real = 0          # porta efetiva após bind

    # ── Discovery ─────────────────────────────────────────────────────────────
    def _build_response(self):
        resp               = smart_city_pb2.DiscoveryResponse()
        resp.device_id     = self.device_id
        resp.type          = self.device_type
        resp.ip            = '127.0.0.1'
        resp.tcp_port      = self._tcp_port_real   # porta real, não a hint
        resp.initial_state = 'ATIVO' if self.active else 'INATIVO'
        return resp

    def _listen_for_discover(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        s           = smart_city_pb2.DataStream()
        s.device_id = sub_id
        s.value     = float(value)
        s.timestamp = int(time.time())
        try:
            self._data_sock.sendto(s.SerializeToString(), (GATEWAY_IP, DATA_PORT))
        except Exception:
            pass

    # ── Comandos TCP ──────────────────────────────────────────────────────────
    def _handle_command(self, conn, addr):
        try:
            data = conn.recv(1024)
            cmd  = smart_city_pb2.Command()
            if data:
                cmd.ParseFromString(data)
            self._apply_command(cmd)
        except Exception as e:
            print(f"[{self.device_id}] Erro no comando: {e}")
        finally:
            conn.close()

    def _apply_command(self, cmd):
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
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # bind na porta hint (0 = SO escolhe uma livre)
        srv.bind(('', self._tcp_port_hint))
        srv.listen(5)
        srv.settimeout(1.0)

        # descobre e registra a porta real que foi atribuída
        self._tcp_port_real = srv.getsockname()[1]
        print(f"[{self.device_id}] Comandos TCP na porta {self._tcp_port_real} "
              f"({'dinâmica' if self._tcp_port_hint == 0 else 'fixa'})...")

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
        # Sensores sem TCP (contínuos): tcp_port_hint < 0 → sem servidor
        if self._tcp_port_hint >= 0:
            t = threading.Thread(target=self._listen_for_commands, daemon=True)
            t.start()
            # aguarda o bind acontecer antes de iniciar o discovery,
            # garantindo que _tcp_port_real já está preenchido quando
            # o primeiro DiscoveryResponse for enviado
            time.sleep(0.3)

        threading.Thread(target=self._listen_for_discover, daemon=True).start()
        time.sleep(0.2)

        try:
            self._data_loop()
        except KeyboardInterrupt:
            self.running = False
            self._data_sock.close()
            print(f"\n[{self.device_id}] Desligado.")
