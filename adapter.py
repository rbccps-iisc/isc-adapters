from MQTTPubSub import MQTTPubSub
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
import time
import sys
import json
import base64
from multiprocessing import Process
from redis import Redis
from rq import Queue
import zmq
import os
import importlib.machinery

adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}
items = {}


try:
    with open(cwd + '/items.json', 'r') as f:
        items = json.load(f)
        for item in items.keys(): 
            try:
                modules[item] = {}
                modules[item]["protoFrom"]["object"] = getattr(importlib.machinery.SourceFileLoader('from_pb2',adaptersDir + '/id_' + item + '/from_pb2.py').load_module(), items[item]["protoFrom"])
                modules[item]["protoTo"]["object"] = getattr(importlib.machinery.SourceFileLoader('to_pb2',adaptersDir + '/id_' + item + '/to_pb2.py').load_module(), items[item]["protoTo"])
            except:
                print("Couldn't load", item)
except:
    print("Couldn't load")




def server():
    
    
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % 5555)
    while True:
        message = socket.recv()
        print("Received request  %s" %  message)
        itemEntry = json.loads(str(message,'utf-8'))
        itemId = list(itemEntry.keys())[0]
        modules[itemId] = {}
        modules[itemId]["protoFrom"] = getattr(importlib.machinery.SourceFileLoader('from_pb2',adaptersDir + '/id_' + itemId + '/from_pb2.py').load_module(), itemEntry["protoFrom"])
        modules[itemId]["protoTo"] = getattr(importlib.machinery.SourceFileLoader('to_pb2',adaptersDir + '/id_' + itemId + '/to_pb2.py').load_module(), itemEntry["protoTo"])
        socket.send_string("ACK")
        
Process(target=server).start()


protosJson = {}




def NSSub_onMessage(mqttc, obj, msg):
       

# NS Message topics are of the form application/{applicationId}/node/{id}/rx
    try:            
        topic = msg.topic 
        topic = topic.split('/')
        itemId = topic[4]
        if itemId in modules:
            ns_sensor_message = modules[itemId]["protoFrom"] 
            jsonData = json.loads((msg.payload).decode("utf-8"))
            decodedData = base64.b64decode(jsonData["data"])
            ns_sensor_message.ParseFromString(decodedData)
            mw_message = MessageToDict(ns_sensor_message) 
            print (mw_message)
            mwPub.publish(itemId, json.dumps(mw_message))
    except:
        print("DECODE ERROR")





def MWSub_onMessage(mqttc, obj, msg):

#Change according to wildcard entry 
    try:
        mw_actuation_message = modules["id"]["protoTo"]
        data = {}
        data['reference'] = 'a'
        data['confirmed'] = False
        data['fport'] = 1
        print (msg.payload)
        json_format.Parse(msg.payload, mw_actuation_message, ignore_unknown_fields=False)
        data['data'] = (base64.b64encode(mw_actuation_message.SerializeToString())).decode("utf-8")
        nsPub.publish(json.dumps(data),)
    except:
        print("DECODE ERROR")


def MWSub_onConnect(client, userdata, flags, rc):
    
    print("Connected to MW SUB with result code "+str(rc))



def MWPub_onConnect(client, userdata, flags, rc):
    
    print("Connected to MW PUB with result code "+str(rc))




def NSSub_onConnect(client, userdata, flags, rc):
    
    print("Connected to NS SUB result code "+str(rc))



def NSPub_onConnect(client, userdata, flags, rc):
    
    print("Connected to NS PUB with result code "+str(rc))







mwSubParams = {}
mwSubParams["url"] = "10.156.14.6"
mwSubParams["port"] = 2333
mwSubParams["timeout"] = 60
mwSubParams["onMessage"] = MWSub_onMessage
mwSubParams["onConnect"] = MWSub_onConnect
mwSubParams["username"] = "admin"
mwSubParams["password"] = "admin@123"
mwSub = MQTTPubSub(mwSubParams)



mwPubParams = {}
mwPubParams["url"] = "10.156.14.6"
mwPubParams["port"] = 2333
mwPubParams["timeout"] = 60
mwPubParams["onConnect"] = MWPub_onConnect
mwPubParams["username"] = "admin"
mwPubParams["password"] = "admin@123"
#mwPubParams["onMessage"] = MWPub_onMessage
mwPub = MQTTPubSub(mwPubParams)







nsSubParams = {}
nsSubParams["url"] = "gateways.rbccps.org"
nsSubParams["port"] = 1883
nsSubParams["timeout"] = 60
nsSubParams["topic"] = "application/1/node/+/rx"
nsSubParams["onMessage"] = NSSub_onMessage
nsSubParams["onConnect"] = NSSub_onConnect
nsSubParams["username"] = "loraserver"
nsSubParams["password"] = "password"
nsSub = MQTTPubSub(nsSubParams)


nsPubParams = {}
nsPubParams["url"] = "gateways.rbccps.org"
nsPubParams["port"] = 1883
nsPubParams["timeout"] = 60
nsPubParams["topic"] = "application/1/node/+/tx"
#nsPubParams["onMessage"] = NSPub_onMessage
nsPubParams["onConnect"] = NSSub_onConnect
nsPubParams["username"] = "loraserver"
nsPubParams["password"] = "password"
nsPub = MQTTPubSub(nsPubParams)








def main():
    mwSub_rc = mwSub.run()
    nsSub_rc = nsSub.run()
	
    mwPub_rc = mwPub.run()
    nsPub_rc = nsPub.run()
	

    while True:
        time.sleep(10)




if __name__ == "__main__":
    main()
