import sys, os, time, random
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sensors._base_sensor import BaseSensor
from proto import smart_city_pb2

class WeatherStation(BaseSensor):
    def __init__(self):
        super().__init__(
            device_id     = 'WeatherStation-Centro',
            device_type   = smart_city_pb2.TEMPERATURE,
            tcp_port      = -1,   # sensor contínuo: sem servidor TCP
            send_interval = 10,
        )
        self._temp  = random.uniform(24.0, 28.0)
        self._humid = random.uniform(55.0, 75.0)

    def _next_temp(self):
        self._temp += random.uniform(-0.5, 0.5)
        self._temp  = max(15.0, min(45.0, self._temp))
        return round(self._temp, 2)

    def _next_humid(self):
        self._humid += random.uniform(-1.0, 1.0)
        self._humid  = max(20.0, min(100.0, self._humid))
        return round(self._humid, 1)

    def _data_loop(self):
        print(f"[{self.device_id}] Enviando dados a cada {self.send_interval}s...")
        while self.running:
            temp  = self._next_temp()
            humid = self._next_humid()
            self._send_stream(f"{self.device_id}:temperatura", temp)
            self._send_stream(f"{self.device_id}:umidade",     humid)
            print(f"[{self.device_id}] Temp: {temp}°C | Umidade: {humid}%")
            time.sleep(self.send_interval)

if __name__ == '__main__':
    WeatherStation().start()
