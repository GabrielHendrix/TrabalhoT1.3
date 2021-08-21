from paho.mqtt import client as mqtt_client
from HashTable import HashTable
import threading
import random
import time

broker = 'broker.emqx.io'
port = 1883
topic = 'HashTable'

def connect_mqtt(client_id) -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Node " + str(client_id) + " connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

class DHT:
    def __init__(self):
        self.listThreads=[]
        self.listMQTTT=[]

    def subscribe(self, client: mqtt_client, key, hashTable):
        def on_message(client, userdata, msg):
            """ print(f"Node `{key}` received `{msg.payload.decode()}`") """
            if msg.payload.decode().split('(')[0] == 'put':
                self.command = ((str(msg.payload.decode()).split('(')[1]).split(')')[0]).split(',')[0]
 
                try:
                    if self.range(int(key)):
                        hashTable.put(int(self.command), 
                                    ((str(msg.payload.decode()).split('(')[1]).split(')')[0]).split(',')[1])
                        #time.sleep(1)
                        print("\n\nThe key `" + str(int(self.command)) + "` with the value `" + 
                            str(((msg.payload.decode().split('(')[1]).split(')')[0]).split(',')[1]) + 
                            "` has been inserted into the node `" + str(key) + "`")
                        """ print(hashTable.getKeys())
                        print(key) """
                        self.command = ''
                except:
                    pass
                    
            if msg.payload.decode().split('(')[0] == 'get':
                self.command = str((msg.payload.decode().split('(')[1]).split(')')[0])
        
                try:
                    if self.range(int(key)):
                        print("Node `" + str(key) +"`: The string value stored in key `" + str(self.command) + "` is: `" + str(hashTable.get(int(self.command))) + "`")
                        #print(hashTable.getKeys())
                        self.command = ''
                except:
                    pass
                

        client.subscribe(topic)
        client.on_message = on_message
        
    def range(self,key):
        for index, val in enumerate(self.listThreads):
            if (index + 1) < len(self.listThreads):
                if key == val and (int(self.command) <= val and int(self.command) > self.listThreads[index + 1]):
                    return True
            if (index + 1) == len(self.listThreads):
                if key == val and (int(self.command) <= val or 
                    (key == val and int(self.command) >= self.listThreads[0])):
                    return True

    def run(self, client_id, hashTable):
        client = connect_mqtt(client_id)
        self.subscribe(client, client_id, hashTable)
        client.loop_forever()

    def join(self):
        key = random.randint(0,pow(2,32))
        while True:
            if (key in self.listThreads):
                key = random.randint(0,pow(2,32))
            else:
                print("Created Node " + str(key))
                hashTable = HashTable()
                x = threading.Thread(target=self.run, args=(str(key), hashTable,))
                self.listMQTTT.append(x)
                x.start()
                break

        self.listThreads.append(key)        
       
    def bigger(self,x,y):
        max = x

        if y > max:
            max = y

    def minor(self,x,y):
        min = x

        if y < min:
            min = y
        return min

    def dht_server(self):
        threads = []
        self.command = ''
        for _ in range(8):
            x = threading.Thread(target=self.join)
            threads.append(x)
            x.start()
        time.sleep(3)
        self.listThreads = sorted(self.listThreads, reverse=True)   
        
        print(self.listThreads)  

def main():
    m = DHT()
    m.dht_server()

if __name__ == "__main__":
    main()