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

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia ciclo de vida da aplicação, incluindo a conexão com o RabbitMQ"""
    global rabbitmq_connection, rabbitmq_channel
    print("Iniciando a aplicação e conectando ao RabbitMQ...")
    try:
        # Conecta ao RabbitMQ (reconexão robusta para lidar com falhas)
        rabbitmq_connection = await aio_pika.connect_robust(RABBITMQ_URL)
        rabbitmq_channel = await rabbitmq_connection.channel()

        # Declara a fila duravel (persistente) para garantir que os dados não sejam perdidos
        await rabbitmq_channel.declare_queue(QUEUE_NAME, durable=True)
        print(f"Conectado ao RabbitMQ e fila '{QUEUE_NAME}' declarada com sucesso.")   
        yield  # Permite que a aplicação continue rodando
    except Exception as e:
        print(f"Erro ao conectar ao RabbitMQ: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao conectar ao RabbitMQ")
    finally:
        # Fecha a conexão com o RabbitMQ ao encerrar a aplicação
        if rabbitmq_connection:
            await rabbitmq_channel.close()
            await rabbitmq_connection.close()
            print("Conexão com o RabbitMQ fechada.")

app = FastAPI(
    title="Iot Telemetry Ingestion API",
    description="API para ingestão de dados de telemetria de dispositivos IoT, utilizando RabbitMQ como broker de mensagens.",
    lifespan=lifespan)

@app.post("/api/v1/telemetry",status_code=status.HTTP_202_ACCEPTED)
async def receive_telemetry(data: TelemetryData):
    """Endpoint publico chamado pelos sensores IoT para enviar dados de telemetria. Os dados são enviados para o RabbitMQ."""
    try:
        # import pdb;pdb.set_trace()
        #Converte o contrato Pydantic para JSON
        message_body = data.model_dump_json().encode('utf-8')

        #Publica a mensagem na fila do RabbitMQ
        await rabbitmq_channel.default_exchange.publish(
            aio_pika.Message(body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT # Grava a mensagem em disco no broker
            ),
            routing_key=QUEUE_NAME
        )

        print(f"[{data.timestamp}] Telemetria recebida e publicada na fila: {data.codigo_hardware}")
        return {"status": "accepted","sensor":data.codigo_hardware,"message": "Telemetria recebida e publicada na fila com sucesso."}
    except Exception as e:
        print(f"Erro ao processar a telemetria: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao inserir mensagem na fila: {str(e)}")
    