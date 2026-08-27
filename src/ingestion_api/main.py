import json
from contextlib import asynccontextmanager
import aio_pika
from fastapi import FastAPI, HTTPException, status
from src.common.schemas import TelemetryData

# Configurações do Broker (RabbitMQ rodando no Docker)
RABBITMQ_URL = "amqp://admin:adminpassword@localhost:5672/"
QUEUE_NAME = "telemetry_queue"

# Variáveis globais para reuso da conexão
rabbitmq_connection = None
rabbitmq_channel = None

