import asyncio
import aio_pika
import asyncpg
import redis.asyncio as aioredis
from src.common.schemas import TelemetryData
from motor.motor_asyncio import AsyncIOMotorClient

RABBITMQ_URL = "amqp://admin:adminpassword@localhost:5672/"
REDIS_URL = "redis://localhost:6379"
MONGO_URL = "mongodb://admin:adminpassword@localhost:27017/?authSource=admin"
POSTGRES_URL = "postgresql://admin:adminpassword@localhost:5432/postgres"

QUEUE_NAME = "telemetry_queue"
MONGO_DB_NAME = "iot_telemetry"
MONGO_COLLECTION_NAME = "sensor_readings"

async def process_message(message: aio_pika.IncomingMessage, redis_client: aioredis.Redis, mongo_collection, pg_pool: asyncpg.Pool = None):
    """Processa cada mensagem vinda da fila e atualiza a persistência"""
    async with message.process(requeue=True): # Envia ACK automático ao final deste bloco; devolve a fila se falhar
        # Desserializa e valida dados 
        payload_bytes = message.body
        telemetry = TelemetryData.model_validate_json(payload_bytes)  # Valida e desserializa os dados recebidos

        # Coverte para dics para injetar regra
        dados = telemetry.model_dump(mode='dict')

        # 1. Consulta regras no PGS
        query = "SELECT umidade_minima, temperatura_maxima from sensor_rules WHERE codigo_hardware = $1"
        regra = await pg_pool.fetchrow(query, telemetry.codigo_hardware)
        # 2. Avalia a regra
        alerta = False
        if regra:
            if telemetry.payload.umidade_solo < regra['umidade_minima'] or \
               telemetry.payload.temperatura_ar > regra['temperatura_maxima']:
                alerta = True
                print(f"🚨 ALERTA CRÍTICO: Sensor {telemetry.codigo_hardware} fora dos limites!")

        # 3. Injeta o status de alerta nos dados do Redis
        dados["alerta_ativo"] = alerta
        
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
            f"Salvo (Alerta: {alerta})"
        )

async def start_worker():
    """Conecta aos brokers e escuta a fila continuamente"""
    print("Starting conection to RabbitMQ, Redis, MongoDB...")
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    mongo_db = mongo_client[MONGO_DB_NAME]
    mongo_collection = mongo_db[MONGO_COLLECTION_NAME]

    # Criação do Pool de conexões do Postgres
    pg_pool = await asyncpg.create_pool(POSTGRES_URL)

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()

    # avoid memory overload 
    await channel.set_qos(prefetch_count=10)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    print("Worker is now listening for messages...")

    # Loop listening for messages
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await process_message(message, redis_client, mongo_collection, pg_pool)

if __name__ == "__main__":
    try:
        asyncio.run(start_worker())
    except KeyboardInterrupt:
        print("Worker stopped by user.")