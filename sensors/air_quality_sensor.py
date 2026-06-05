import sys, os, time, random
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from sensors._base_sensor import BaseSensor
from proto import smart_city_pb2

class AirQualitySensor(BaseSensor):
    def __init__(self):
        super().__init__(
            device_id     = 'AirQuality-Industrial',
            device_type   = smart_city_pb2.AIR_QUALITY,
            tcp_port      = 6003,
            send_interval = 20,
        )
        self.alert_threshold = 800.0   # ppm CO2
        self._co2 = random.uniform(400.0, 500.0)

    def _apply_command(self, cmd):
        super()._apply_command(cmd)
        if cmd.action == smart_city_pb2.Command.SET_THRESHOLD:
            self.alert_threshold = cmd.parameter
            print(f"[{self.device_id}] Limiar CO2 → {self.alert_threshold} ppm")

    def _next_co2(self):
        if random.random() < 0.05:
            self._co2 += random.uniform(150, 300)
        else:
            self._co2 += random.uniform(-20, 20)
        self._co2 = max(350.0, min(1200.0, self._co2))
        return round(self._co2, 1)

    def _data_loop(self):
        print(f"[{self.device_id}] Enviando dados a cada {self.send_interval}s...")
        while self.running:
            if not self.active:
                time.sleep(2)
                continue
            co2      = self._next_co2()
            is_alert = co2 >= self.alert_threshold
            self._send_stream(self.device_id, co2)
            sufixo = " ⚠ ALERTA DE CO2" if is_alert else ""
            print(f"[{self.device_id}] CO2: {co2} ppm{sufixo}")
            time.sleep(1 if is_alert else self.send_interval)

if __name__ == '__main__':
    AirQualitySensor().start()
