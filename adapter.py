from MQTTPubSub import MQTTPubSub
from AMQPPubSub import AMQPPubSub
from time import sleep
import redis, hiredis
import base64
from celery import Celery
import zmq
import json
import importlib.machinery
import os
from multiprocessing import Process
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
import ast
import pymongo

redConn = redis.StrictRedis(host='localhost', port=6379, db=0)

#----------------------------------------------------------------------------------------------------#

#MongoDB setup

client=pymongo.MongoClient()

db=client.devices_db                                    #DB OF DEVICES

cln=db.devices                                          #COLLECTION OF DEVICES REGISTERED

#----------------------------------------------------------------------------------------------------#

#Code for celery tasks to be implemented when messages arrive to the MQTT topic(s)

celery_app=Celery('Adapter', broker='redis://localhost/0')

@celery_app.task
def decode_push():
    global red
    mes_pay=redConn.lpop("incoming-messages")
    mes_dict=ast.literal_eval(mes_pay.decode('utf-8'))
    dec_payload=list(mes_dict.values())[0]
    dec_device_id=list(mes_dict.keys())[0]
    print(mes_dict)
    #code for proto decode
    try:
        if dec_device_id in modules:
            ns_sensor_message = modules[dec_device_id]["protoFrom"]
            jsonData = json.loads(dec_payload)
            b64en=jsonData["data"]
            decodedData = base64.b64decode(b64en.encode('utf-8'))
            ns_sensor_message.ParseFromString(decodedData)
            mw_message = MessageToDict(ns_sensor_message)
            mwSub.publish(dec_device_id,json.dumps(mw_message))
            print("Published", mw_message)
    except Exception as e:
        print("DECODE ERROR")
        print(e)

@celery_app.task
def encode_push():
    global red
    out_dict=ast.literal_eval(redConn.lpop("outgoing-messages").decode('utf-8'))
    en_body=(list(out_dict.values())[0])
    en_id=list(out_dict.keys())[0]
    try:
        mw_actuation_message = modules[en_id]["protoTo"]
        data = {}
        data["reference"] = "a"
        data["confirmed"] = False
        data["fport"] = 1
        print(en_body)
        json_format.Parse(en_body, mw_actuation_message, ignore_unknown_fields=False)
        data["data"] = base64.b64encode(mw_actuation_message.SerializeToString())
        nsSub.publish(ns_tx_topic.replace("{id}", _id), json.dumps(data))

    except Exception as e:
        print("ENCODE ERROR")
        print(e)

#----------------------------------------------------------------------------------------------------#

#Defining various callbacks for MQTT and AMQP connections

def MWSub_onMessage(ch, method, properties, body):
    print(body)
    #Change according to wildcard entry 
#    _id = method.routing_key.replace('_update','')
#    am_dict={}
#    am_dict={_id:body.decode('utf-8')}
#    redConn.rpush("outgoing-messages", am_dict)
#    print("Received", am_dict)
#    encode_push.delay()
#_____!!!!!!ADD A CHECK FOR HTTP DEVICES UP THERE, IN CASE IT IS IMPLEMENTED IN FUTURE!!!!!!_____#





def NSSub_onConnect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))



def NSSub_onMessage(client, userdata, msg):
    global r
    in_str=json.dumps(json.loads(msg.payload.decode('utf-8')))
    mq_dict={}
    mq_dict={msg.topic.split('/')[3]:in_str}
    redConn.rpush("incoming-messages", mq_dict)
    print("Pushed", mq_dict)
    decode_push.delay()

#----------------------------------------------------------------------------------------------------#

#To import proto files to the code 

adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}
items = {}

ns_rx_topic = "application/1/node/{id}/rx"
ns_tx_topic = "application/1/node/{id}/tx"

validationFlag = False


try:
        res=cln.find(projection={'_id': False})
        for ids in res:
            items.update(ids)
#---!!! NEXT LINE HAS BEEN ADDED FOR OFFLINE TESTING, ALONG WITH SEVERAL OTHERS. DELETE THEM DURING INTEGRATION !!!---
        items={"70b3d58ff0031de5": {"protoTo": "_targetConfigurations", "protoFrom": "sensor_values", "id": "70b3d58ff0031de5"}}
        
        for item in list(items.keys()): 
            try:
                modules[item] = {}
                from_spec = importlib.util.spec_from_file_location('from_' + item + '_pb2', adaptersDir + '/id_' + item + '/from_' + item + '_pb2.py')
                from_mod = importlib.util.module_from_spec(from_spec)
                from_spec.loader.exec_module(from_mod)
                modules[item]["protoFrom"] = getattr(from_mod,items[item]["protoFrom"])()

                to_spec = importlib.util.spec_from_file_location('to_' + item + '_pb2', adaptersDir + '/id_' + item + '/to_' + item + '_pb2.py')
                to_mod = importlib.util.module_from_spec(to_spec)
                to_spec.loader.exec_module(to_mod)
                modules[item]["protoTo"] = getattr(to_mod, items[item]["protoTo"])()
            except Exception as e:
                print("Couldn't load", item)
                print(e)
except Exception as e:
    print("Couldn't load")



def server():
    
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket.bind("tcp://*:%s" % 1617)
    while True:
        message = socket.recv()
        print("Received request  %s" %  message)
        itemEntry = json.loads(str(message,'utf-8'))
        itemId = itemEntry["id"]
        modules[itemId] = {}
        try:
            from_spec = importlib.util.spec_from_file_location('from_' + itemId + '_pb2', adaptersDir + '/id_' + itemId + '/from_' + itemId + '_pb2.py')
            from_mod = importlib.util.module_from_spec(from_spec)
            from_spec.loader.exec_module(from_mod)
            modules[itemId]["protoFrom"] = getattr(from_mod, itemEntry["protoFrom"])()
            to_spec = importlib.util.spec_from_file_location('to_' + itemId + '_pb2', adaptersDir + '/id_' + itemId + '/to_' + itemId + '_pb2.py')
            to_mod = importlib.util.module_from_spec(to_spec)
            to_spec.loader.exec_module(to_mod)
            modules[itemId]["protoTo"] = getattr(to_mod, itemEntry["protoTo"])()
        except:
            print("Couldn't load objects")

Process(target=server).start()

protosJson = {}

#----------------------------------------------------------------------------------------------------#

#Connection parameters for AMQP and MQTT connections

mwSubParams = {}
#mwSubParams["url"] = "10.156.14.6"
mwSubParams["url"] = "localhost"
mwSubParams["port"] = 5672
mwSubParams["timeout"] = 60
mwSubParams["onMessage"] = MWSub_onMessage
#mwSubParams["username"] = "admin"
#mwSubParams["password"] = "admin@123"
mwSubParams["exchange"] = "amq.topic"
mwSub = AMQPPubSub(mwSubParams)

#---!!! DON'T FORGET TO UNCOMMENT LINES IN AMQPPubSub.py FILE DURING INTEGRATION !!!---

nsSubParams = {}
#nsSubParams["url"] = "gateways.rbccps.org"
nsSubParams["url"] = "localhost"
nsSubParams["port"] = 1883
nsSubParams["timeout"] = 60
nsSubParams["topic"] = "application/1/node/+/rx"
nsSubParams["onMessage"] = NSSub_onMessage
nsSubParams["onConnect"] = NSSub_onConnect
#nsSubParams["username"] = "loraserver"
#nsSubParams["password"] = "loraserver"
nsSub = MQTTPubSub(nsSubParams)


#----------------------------------------------------------------------------------------------------#


def main():
    mwSub_rc = mwSub.run()
    nsSub_rc = nsSub.run()

    while True:
        sleep(10)

if __name__=="__main__":
    main()
