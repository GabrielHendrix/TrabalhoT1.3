import random
from paho.mqtt import client as mqtt_client
import time

broker = 'broker.emqx.io'
port = 1883
topic = "HashTable"

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client('0')
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, msg):
    time.sleep(1)
    result = client.publish(topic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")

def run():
    client = connect_mqtt()
    command = ''
    command = input('Digite `Join` para inicializar a DHT: ')
    while command.split('(')[0] != 'Join':
        break
    while command != 'quit':
        command = input('Digite um comando (put() / get()) ou "quit" para encerrar: ')
        if command.split('(')[0] == 'put':
            publish(client, command)
        if command.split('(')[0] == 'get':
            publish(client, command)
        command = ''

if __name__ == '__main__':
    run()


