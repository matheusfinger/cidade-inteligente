import sys, os, time, random
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sensors._base_sensor import BaseSensor
from proto import smart_city_pb2

class TrafficLight(BaseSensor):
    def __init__(self):
        super().__init__(
            device_id     = 'TrafficLight-AvenidaCentral',
            device_type   = smart_city_pb2.TRAFFIC_LIGHT,
            tcp_port      = 6002,
            send_interval = 15,
        )
        self.alert_threshold = 40.0   # veículos/min

    def _apply_command(self, cmd):
        super()._apply_command(cmd)   # TURN_ON, TURN_OFF, CHANGE_FREQ
        if cmd.action == smart_city_pb2.Command.SET_THRESHOLD:
            self.alert_threshold = cmd.parameter
            print(f"[{self.device_id}] Limiar → {self.alert_threshold} veíc/min")

    def _next_traffic(self):
        hora = time.localtime().tm_hour
        if 7 <= hora <= 9 or 17 <= hora <= 19:
            base = random.uniform(35, 60)
        elif hora >= 22 or hora <= 5:
            base = random.uniform(2, 10)
        else:
            base = random.uniform(15, 35)
        return round(base + random.uniform(-3, 3), 1)

    def _data_loop(self):
        print(f"[{self.device_id}] Enviando dados a cada {self.send_interval}s...")
        while self.running:
            if not self.active:
                time.sleep(2)
                continue
            veiculos  = self._next_traffic()
            is_alert  = veiculos >= self.alert_threshold
            self._send_stream(self.device_id, veiculos)
            sufixo = " ⚠ ALERTA" if is_alert else ""
            print(f"[{self.device_id}] Tráfego: {veiculos} veíc/min{sufixo}")
            time.sleep(1 if is_alert else self.send_interval)

if __name__ == '__main__':
    TrafficLight().start()
