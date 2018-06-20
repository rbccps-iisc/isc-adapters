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
from jsonschema import validate
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

app_decode=Celery('Decode', broker='redis://localhost/0')

@app_decode.task
def decode_push():
    global red
    inc_dict=ast.literal_eval(redConn.lpop("incoming-messages").decode('utf-8'))
    payload=(inc_dict.keys()[0]).decode('utf-8')
    device_id=inc_dict.values()[0]
    #code for proto decode
    try:
        if device_id in modules:
            ns_sensor_message = modules[device_id]["protoFrom"]
            jsonData = json.loads(payload)
            decodedData = base64.b64decode(jsonData["data"])
            ns_sensor_message.ParseFromString(decodedData)
            mw_message = MessageToDict(ns_sensor_message)
            print (mw_message)
            mwSub.publish(itemId,json.dumps(mw_message))
    except Exception as e:
        print("DECODE ERROR")
        print(e)

#----------------------------------------------------------------------------------------------------#

#Defining various callbacks for MQTT and AMQP connections

def MWSub_onMessage(ch, method, properties, body):

    #Change according to wildcard entry 
    try:
        _id = method.routing_key.replace('_update','')
        print(_id)
        mw_actuation_message = modules[_id]["protoTo"]
        print('Received ', _id, ' from MW')
        data = {}
        data['reference'] = 'a'
        data['confirmed'] = False
        data['fport'] = 1
        print(body)
        json_format.Parse(body, mw_actuation_message, ignore_unknown_fields=False)
        data['data'] = (base64.b64encode(mw_actuation_message.SerializeToString())).decode("utf-8")
        schema(json=data['data'], devId= _id)
        if(validationFlag):
            nsSub.publish(ns_tx_topic.replace("{id}", _id), json.dumps(data))

    except Exception as e:
        print("ENCODE ERROR")
        print(e)

#_____!!!!!!ADD A CHECK FOR HTTP DEVICES UP THERE, IN CASE IT IS IMPLEMENTED IN FUTURE!!!!!!_____#

def NSSub_onConnect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))



def NSSub_onMessage(client, userdata, msg):
    global r
    mq_dict={}
    mq_dict={msg.payload.decode('utf-8'):msg.topic.split("/")[3]}
    redConn.rpush("incoming-messages", mq_dict)
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
##    with open(cwd + '/items.json', 'r') as f:
##        items = json.load(f)		#LOAD JSON OBJECTS
        res=cln.find(projections={'_id':FALSE})
        for ids in res:
            items.update(ids)

        for item in items.keys(): 
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
except:
    print("Couldn't load")



def schema(json, devId):

    print('in function schema.....')
    with urllib.request.urlopen('https://smartcity.rbccps.org/api/0.1.0/cat') as resp:
        data = json.loads(resp.read().decode())
        sensor_data = data['items']
        data.clear()

    sensor_schema = {}
    for i in range(0, len(sensor_data)):
        sensor_schema[sensor_data[i]['id']] = sensor_data[i]['data_schema']

    try :
        validate(instance= json, schema= sensor_schema[devId])
        validationFlag = True

    except Exception as e:
        print('given device data is not valid to its schema..')
        validationFlag = False



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
mwSubParams["url"] = "10.156.14.6"
mwSubParams["port"] = 5672
mwSubParams["timeout"] = 60
mwSubParams["onMessage"] = MWSub_onMessage
mwSubParams["username"] = "admin"
mwSubParams["password"] = "admin@123"
mwSubParams["exchange"] = "amq.topic"
mwSub = AMQPPubSub(mwSubParams)



nsSubParams = {}
nsSubParams["url"] = "gateways.rbccps.org"
nsSubParams["port"] = 1883
nsSubParams["timeout"] = 60
nsSubParams["topic"] = "application/1/node/+/rx"
nsSubParams["onMessage"] = NSSub_onMessage
nsSubParams["onConnect"] = NSSub_onConnect
nsSubParams["username"] = "loraserver"
nsSubParams["password"] = "loraserver"
nsSub = MQTTPubSub(nsSubParams)


#----------------------------------------------------------------------------------------------------#


def main():
    mwSub_rc = mwSub.run()
    nsSub_rc = nsSub.run()



    while True:
        sleep(10)

if __name__=="__main__":
    main()
