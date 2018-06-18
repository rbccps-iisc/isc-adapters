import asyncio
import requests
from AMQPPubSub import AMQPPubSub
from time import sleep
import redis, hiredis
import base64 as bs
from celery import Celery
import zmq
import json
import importlib.machinery
import os
import sys
from multiprocessing import Process
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
from jsonschema import validate
import ast

#---------------------------------------------------------------------------------------------------------------------

#Code for celery tasks to be implemented when messages arrive to the MQTT topic(s)

redConn = redis.StrictRedis(host='localhost', port=6379, db=0)

appd=Celery('print', broker='redis://localhost/0')

@appd.task
def decode_push():
    global red
    inc_dict=ast.literal_eval(redConn.lpop("HTTP-messages").decode('utf-8'))
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

#------------------------------------------------------------------------------------------------------------------------

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
		#------!!!ADD link for posting to HTTP SERVER!!!------
    except Exception as e:
        print("ENCODE ERROR")
        print(e)


#-------------------------------------------------------------------------------------------------------------------------

#To import proto files to the code 

adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}
items = {}

validationFlag = False

try:
    with open(cwd + '/items.json', 'r') as f:
        items = json.load(f)
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
    socket.bind("tcp://*:%s" % 5555)
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
            print(sys.exc_traceback.tb_lineno)

Process(target=server).start()

protosJson = {}

#----------------------------------------------------------------------------------------------

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

#-------------------------------------------------------------------------------------------------------------------------------

async def server_one():
	while True:
		requests.get("")							#------!!!CONFIGURE THIS!!!------
		if r.status_code==requests.status.ok:
			http_dict=[:r.text]						#------!!!CONFIGURE THIS!!!------
			redConn.push("HTTP-messages", http_dict)
			decode_push.delay()
			await asyncio.sleep(0)						#------!!!CONFIGURE THIS!!!------

def main():
	ioloop = asyncio.get_event_loop()
	mwSub_rc = mwSub.run()
	tasks = [ioloop.create_task(server_one()), ioloop.create_task(bar())]		#------!!!CONFIGURE THIS!!!------
	wait_tasks = asyncio.wait(tasks)
	ioloop.run_until_complete(wait_tasks)
	ioloop.close(


if __name__=="__main__":
    main())
