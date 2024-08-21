import pika
import json
import random
import time

def emit_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='mqtt_exchange', exchange_type='topic')

    while True:
        status = random.randint(0, 6)
        message = json.dumps({'status': status})
        channel.basic_publish(exchange='mqtt_exchange', routing_key='mqtt.status', body=message)
        print(f"Sent: {message}")
        time.sleep(1)

if __name__ == "__main__":
    emit_message()
