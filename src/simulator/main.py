import random
import time
import httpx
from datetime import datetime
from src.common.schemas import TelemetryData, TelemetryPayload

# Config API de Ingestão (Container FastAPI)
API_URL = "http://localhost:8000/api/v1/telemetry"

# Lista de Sensores

SENSORS =  [
    {"hardware_id": "SN-AGRO-001", "type": "Milho - Talhão 1", "battery": 100},
    {"hardware_id": "SN-AGRO-002", "type": "Milho - Talhão 2", "battery": 95},
    {"hardware_id": "SN-AGRO-003", "type": "Soja - Talhão 1", "battery": 80},
    {"hardware_id": "SN-AGRO-004", "type": "Soja - Talhão 2", "battery": 15},
]

def generate_sensor_data(sensor: dict) -> TelemetryData:
    """Gera leituras sintéticas baseadas no estado atual do sensor"""
    # Simula oscilações de solo e clima
    umidade = round(random.uniform(15.0, 45.0),2)
    temperatura = round(random.uniform(22.0, 38.0),2)

    # Simula consumo lento da bateria
    if random.random() < 0.1 and sensor["battery"] > 0:
        sensor["battery"] -= random.randint(1, 3)

    payload = TelemetryPayload(
        umidade_solo=umidade,
        temperatura_ar=temperatura,
        bateria_porcentagem=sensor["battery"]
    )

    return TelemetryData(
        codigo_hardware=sensor["hardware_id"],
        timestamp=datetime.now(),
        payload=payload
    )

def run_simulation(interval_second: int = 3):
    """Loop de envio continuo de telemetria"""
    print("Iniciando Simulador de Telemetria...")

    with httpx.Client(timeout=5.0) as client:
        while True:
            for sensor in SENSORS:
                telemetry = generate_sensor_data(sensor)

                data_json = telemetry.model_dump(mode = 'json')

                try:
                    # HTTP POST 
                    response = client.post(API_URL, json=data_json)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Enviado: {telemetry.codigo_hardware} | Status: {response.status_code}")
                except httpx.RequestError as e:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Erro ao enviar dados de {telemetry.codigo_hardware}: {e}")

            time.sleep(interval_second)

if __name__ == "__main__":
    run_simulation(interval_second=0.2)