from datetime import datetime 
from pydantic import BaseModel, Field

class TelemetryPayload(BaseModel):
    """sub modelo com as leituras fisicas do senso"""
    umidade_solo: float = Field(...,ge = 0 ,le = 100,description="Umidade do solo em porcentagem")
    temperatura_ar: float = Field(...,ge = 10, le = 60,description="Temperatura do ar em graus Celsius")
    bateria_porcentagem: int = Field(...,ge = 0, le = 100,description="Porcentagem da bateria do sensor/hardware")

class TelemetryData(BaseModel):
    """Contrato entre sistemas enviado pelo simulador/API """
    codigo_hardware: str = Field(...,min_lenght=5,description="Codigo do hardware/sensor")
    timestamp: datetime = Field(...,description="Data e hora da leitura do sensor")
    payload: TelemetryPayload