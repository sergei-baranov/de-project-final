# это вспомогательный скрипт - посмотреть, заоаботал ли провайдер сообщений в поток.
# рабочий воркер - рядом: stage_stream.py

import time
import json
from typing import Dict, Optional
from confluent_kafka import Consumer

def error_callback(err):
    print('Something went wrong: {}'.format(err))


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            # 'debug': 'all',
            'client.id': 'someclientkey'
        }

        self.consumer = Consumer(params)
        self.consumer.subscribe([topic])

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.consumer.poll(timeout=timeout)

        if msg is None:
            pass
        elif msg.error():
            raise Exception(msg.error())
        else:
            # декодирование и печать сообщения
            val = msg.value().decode()
            return json.loads(val)

def main():
    c = KafkaConsumer(
        host='rc1a-sd5jrikpd9jcve1c.mdb.yandexcloud.net',
        port=9091,
        user='producer_consumer',
        password='sprint_11',
        topic='transaction-service-input',
        group='Goo',
        cert_path='/YandexInternalRootCA.crt'
    )

    # запуск бесконечного цикла
    timeout: float = 3.0
    # мне парочки хватит для разбора структуры
    stopper = 0
    border = 7000
    while True:
        # получение сообщения из Kafka
        s = c.consume(timeout=timeout)

        if s is None:
            # если в Kafka сообщений нет, скрипт засыпает на секунду,
            # а затем продолжает выполнение в новую итерацию
            time.sleep(1)
            continue
        else:
            # печать сообщения
            print(s)
            stopper += 1
            if stopper > border:
                break


if __name__ == '__main__':
    main()
