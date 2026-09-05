import asyncio
import aio_pika
import redis.asyncio as aioredis
from src.common.schemas import TelemetryData
from motor.motor_asyncio import AsyncIOMotorClient

RABBITMQ_URL = "amqp://admin:adminpassword@localhost:5672/"
REDIS_URL = "redis://localhost:6379"
MONGO_URL = "mongodb://admin:adminpassword@localhost:27017/?authSource=admin"

QUEUE_NAME = "telemetry_queue"
MONGO_DB_NAME = "iot_telemetry"
MONGO_COLLECTION_NAME = "sensor_readings"

async def process_message(message: aio_pika.IncomingMessage, redis_client: aioredis.Redis, mongo_collection):
    """Processa cada mensagem vinda da fila e atualiza a persistência"""
    async with message.process(requeue=True): # Envia ACK automático ao final deste bloco; devolve a fila se falhar
        # Desserializa e valida dados 
        payload_bytes = message.body
        telemetry = TelemetryData.model_validate_json(payload_bytes)  # Valida e desserializa os dados recebidos

        # Atualliza a camada de Hot Storage (redis)
        redis_key = f"sensor:{telemetry.codigo_hardware}:status"
        redis_value = telemetry.model_dump_json() 

        # Preparaçãp dos dados para fo MongoDB (Cold Storage / Time-Series)
        mongo_document = telemetry.model_dump(mode='python')

        await asyncio.gather(
            redis_client.set(redis_key, redis_value),
            mongo_collection.insert_one(mongo_document)
        )

        print(
            f"⚙️ [Worker] {telemetry.codigo_hardware} | "
            f"Umitiy: {telemetry.payload.umidade_solo}% -> "
            f"Save in Redis (state) and Mongo (time series)"
        )

async def start_worker():
    """Conecta aos brokers e escuta a fila continuamente"""
    print("Starting conection to RabbitMQ, Redis, MongoDB...")
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    mongo_db = mongo_client[MONGO_DB_NAME]
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]


    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    # avoid memory overload 
    await channel.set_qos(prefetch_count=10)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    print("Worker is now listening for messages...")

    # Loop listening for messages
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await process_message(message, redis_client, mongo_collection)

if __name__ == "__main__":
    try:
        asyncio.run(start_worker())
    except KeyboardInterrupt:
        print("Worker stopped by user.")