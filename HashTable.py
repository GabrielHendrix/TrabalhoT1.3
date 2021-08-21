from hashlib import md5
from LinkedList import *

class HashTable:
    def __init__(self, hashTableSize = 1):
        self.buckets = [LinkedList() for x in range(0, hashTableSize)]
        self.keys = {}

    def new_node(self):
        self.buckets.append(LinkedList())

    def put(self, key, value):

        keyHash = self.hash(key)
        self.keys[key] = keyHash
        bucketLinkedList = self.buckets[keyHash]  
        node = bucketLinkedList.find({key: value})
        
        if node == None:
            bucketLinkedList.prepend({key: value})
        else:
            node.value.value = value

    def get(self, key):

        bucketLinkedList = self.buckets[self.hash(key)]
        currentNode = bucketLinkedList.head
        while not currentNode == None:
            for k in currentNode.value:
                if k == key:
                    return currentNode.value[k]
            currentNode = currentNode.next
        
        return None

    def delete(self, key):

        keyHash = self.hash(key)
        del self.keys[key]
        bucketLinkedList = self.buckets[keyHash]
        node = None
        currentNode = bucketLinkedList.head

        while not currentNode == None and node == None:
            for k in currentNode.value:
                if k == key:
                    node = currentNode
                    break
            currentNode = currentNode.next

        if not node == None:
            return bucketLinkedList.delete(node.value)

        return None
    
    def hash(self, key):
        k = 0
        for s in list(md5(str(key).encode('utf-8')).hexdigest()):
            k += ord(s)
        return k % len(self.buckets)

    def has(self, key):
        if key in self.keys.keys():
            return True
        else:
            return False

    def getKeys(self):
        return list(self.keys.keys())
