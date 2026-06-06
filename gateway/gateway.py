import socket
import threading
import time
import sys
import os
import sqlite3

pasta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(pasta_raiz)

from proto import smart_city_pb2

MULTICAST_GROUP    = '224.0.0.1'
MULTICAST_PORT     = 5000
GATEWAY_IP         = '0.0.0.0'
GATEWAY_PORT       = 5001
DATA_PORT          = 5002
DISCOVERY_INTERVAL = 15
OFFLINE_TIMEOUT    = 45
DB_PATH            = os.path.join(os.path.dirname(__file__), 'gateway.db')

dispositivos_ativos = {}
dispositivos_lock   = threading.Lock()

# ── Banco de dados ────────────────────────────────────────────────────────────
# Usamos uma conexão por thread (check_same_thread=False + lock próprio)
# para evitar erros de concorrência do sqlite3.
db_lock = threading.Lock()

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_lock:
        conn = get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS leituras (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT    NOT NULL,
                value     REAL    NOT NULL,
                timestamp INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_device_ts
                ON leituras (device_id, timestamp);
        """)
        conn.commit()
        conn.close()
    print(f"[Gateway] Banco de dados: {DB_PATH}")

def salvar_leitura(device_id, value, timestamp):
    with db_lock:
        conn = get_conn()
        conn.execute(
            "INSERT INTO leituras (device_id, value, timestamp) VALUES (?, ?, ?)",
            (device_id, value, timestamp)
        )
        conn.commit()
        conn.close()

# ── Consultas analíticas ──────────────────────────────────────────────────────
def consultar_media(device_id, janela_segundos=3600):
    desde = int(time.time()) - janela_segundos
    with db_lock:
        conn = get_conn()
        row = conn.execute(
            "SELECT AVG(value) as media, COUNT(*) as n FROM leituras "
            "WHERE device_id = ? AND timestamp >= ?",
            (device_id, desde)
        ).fetchone()
        conn.close()
    return row['media'], row['n']

def consultar_historico(device_id, limite=20):
    with db_lock:
        conn = get_conn()
        rows = conn.execute(
            "SELECT value, timestamp FROM leituras "
            "WHERE device_id = ? ORDER BY timestamp DESC LIMIT ?",
            (device_id, limite)
        ).fetchall()
        conn.close()
    return rows

# ── Discovery: escuta respostas ───────────────────────────────────────────────
def listen_for_responses():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((GATEWAY_IP, GATEWAY_PORT))
    print(f"[Gateway] Escutando discovery em 0.0.0.0:{GATEWAY_PORT}...")

    while True:
        data, addr = sock.recvfrom(1024)
        response = smart_city_pb2.DiscoveryResponse()
        try:
            response.ParseFromString(data)
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

# ── Dados: escuta DataStream ──────────────────────────────────────────────────
def listen_for_data():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
                for key in [stream.device_id, base_id]:
                    if key in dispositivos_ativos:
                        dispositivos_ativos[key]["last_seen"] = time.time()
            if conhecido:
                print(f"[DATA] {stream.device_id}: {stream.value} (ts: {stream.timestamp})")
                # ── persiste no banco ─────────────────────────────────────────
                salvar_leitura(stream.device_id, stream.value, stream.timestamp)
        except Exception:
            pass

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

# ── Watchdog ──────────────────────────────────────────────────────────────────
def watchdog_loop():
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

# ── Loop principal ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()

    threading.Thread(target=listen_for_responses, daemon=True).start()
    threading.Thread(target=listen_for_data,      daemon=True).start()

    time.sleep(0.5)

    threading.Thread(target=discovery_loop, daemon=True).start()
    threading.Thread(target=watchdog_loop,  daemon=True).start()

    try:
        while True:
            time.sleep(10)
            with dispositivos_lock:
                ids = list(dispositivos_ativos.keys())
            print(f"\n--- Memória do Gateway ({len(ids)} dispositivo(s)) ---")
            for did in ids:
                print(f"  {did}")
            print()
    except KeyboardInterrupt:
        print("\n[Gateway] Desligando...")
