import asyncio
import functools
import logging
import signal
import sys
import json;

import confluent_kafka

signal.signal(signal.SIGTERM, lambda *args: sys.exit())
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def error_callback(err):
    print(f'Something went wrong: {err}')

config = {
    "bootstrap.servers": "rc1a-sd5jrikpd9jcve1c.mdb.yandexcloud.net:9091",
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': '/YandexInternalRootCA.crt',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': '***',
    'sasl.password': '***',
    'group.id': "Goo",
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'error_cb': error_callback,
    # 'debug': 'all',
    'client.id': 'someclientkey'
}


async def consume(config, topic):
    consumer = confluent_kafka.Consumer(config)
    consumer.subscribe([topic])
    loop = asyncio.get_running_loop()
    poll = functools.partial(consumer.poll, 0.1)
    try:
        log.info(f"Starting consumer: {topic}")
        stopper = 0
        border = 7
        while True:
            message = await loop.run_in_executor(None, poll)
            if message is None:
                continue
            if message.error():
                raise Exception(message.error())

            val = message.value().decode()
            s = json.loads(val)
            log.info(f"Consuming message: {s}")

            # в лучшем случае получим 700 (border) сообщений,
            # в худшем - проспим 700 (border) секунд
            stopper += 1
            if stopper > border:
                break
    finally:
        log.info(f"Closing consumer: {topic}")
        consumer.close()


consume = functools.partial(consume, config)


async def main():
    await asyncio.gather(
        consume("transaction-service-input"),
        consume("dds4cdm-services-messages"),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Application shutdown complete")