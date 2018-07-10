'''
ISSUES: Nothing open. Some properties reserved for future usage. Catalog files to be updated.

    Catalog issues:
[]	1. Add ["items"][0]["server_config"]["protocol"] in all devices to indicate protocol used for communication
[]	2. Add ["items"][0]["server_config"]["server_name"] in all devices to indicate server used for communication
[]	3. Add ["items"][0]["device_bosch_api"] in HTTP devices to indicate the API information used for Bosch API validation (7 items inside this)

    Code:
[]	1. Modularise base64+proto, etc
[x]	2. APIs for Bosch
    
'''

from MQTTPubSub import MQTTPubSub
from AMQPPubSub import AMQPPubSub
from time import sleep
import redis
import hiredis
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
import requests
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import threading
from jsontraverse.parser import JsonTraverseParser

redConn = redis.StrictRedis(host='localhost', port=6379, db=0)

scheduler = AsyncIOScheduler()

#----------------------------------------------------------------------------------------------------#

#Initialisation of Bosch Climo APIs

dThingKey = {}

def bosch_init(dev_id):
    
    global dThingKey

    bosch_auth_url = http_items[dev_id]["authUrl"]				    ##"http://52.28.187.167/services/api/v1/users/login"
    bosch_auth_headers = http_items[dev_id]["authHeaders"]			    ##{"Content-Type":"application/json", "Accept":"application/json", "api_key":"apiKey"}
    bosch_auth_payload = http_items[dev_id]["authCred"]			    ##{"password":"Q2xpbW9AOTAz", "username":"SUlTQ19CQU5HQUxPUkU="}
    auth_res = requests.post(url = bosch_auth_url, headers = bosch_auth_headers, data = json.dumps(bosch_auth_payload))

    authToken = auth_res.json()["authToken"]
    OrgKey = auth_res.json()["OrgKey"]


    bosch_thing_url = http_items[dev_id]["thingUrl"]				    ##"http://52.28.187.167/services/api/v1/getAllthings"
    bosch_thing_headers = http_items[dev_id]["thingHeaders"]			    ##{"Accept":"application/json", "api_key":"apiKey", "Authorization":authToken, "X-OrganizationKey":OrgKey}
    thing_res = requests.get(url = bosch_thing_url, headers = bosch_thing_headers)

    dev_thingKey = thing_res.json()["result"][0]["thingKey"]
    
    dThingKey.update({dev_id:dev_thingKey})

#----------------------------------------------------------------------------------------------------#

# MongoDB setup

client = pymongo.MongoClient()

mdb = client.devices_db_mq	    
mcln = mdb.devices		    

hdb = client.devices_db_http
hcln = hdb.devices

#----------------------------------------------------------------------------------------------------#

# Code for celery tasks

celery_app = Celery('Adapter', broker='redis://localhost/0')


@celery_app.task
def decode_push():
    mes_pay = redConn.lpop("incoming-messages")
    mes_dict = ast.literal_eval(mes_pay.decode('utf-8'))
    dec_payload = list(mes_dict.values())[0]
    dec_device_id = list(mes_dict.keys())[0]
    jsonData = json.loads(dec_payload)
    if dec_device_id in http_items:
        b64en = jsonData["data"]
        decodedData = base64.b64decode(b64en.encode('utf-8'))
        mwSub.publish(dec_device_id+'.tx', json.dumps(decodedData.decode('utf-8')))	    ## tx for device transmission
    elif dec_device_id in items:
        # code for proto decode
        try:
            b64en = jsonData["data"]
            decodedData = base64.b64decode(b64en.encode('utf-8'))
            ns_sensor_message = modules[dec_device_id]["protoFrom"]
            ns_sensor_message.ParseFromString(decodedData)
            mw_message = MessageToDict(ns_sensor_message)
            mwSub.publish(dec_device_id+'.tx', json.dumps(mw_message))
            print("Published", mw_message)
        except Exception as e:
            print("DECODE ERROR")
            print(e)


@celery_app.task
def encode_push():
    out_dict = ast.literal_eval(redConn.lpop("outgoing-messages").decode('utf-8'))
    en_body = (list(out_dict.values())[0])
    en_id = list(out_dict.keys())[0]
    try:
        mw_actuation_message = modules[en_id]["protoTo"]
        data = {}
        data["reference"] = "a"
        data["confirmed"] = False
        data["fport"] = 1
        json_format.Parse(en_body, mw_actuation_message, ignore_unknown_fields=False)
        data["data"] = base64.b64encode(mw_actuation_message.SerializeToString()).decode('utf-8')
        nsSub.publish(items[en_id]["postAdd"], json.dumps(data))
    except Exception as e:
        print("ENCODE ERROR")
        print(e)


@celery_app.task
def poll_to_url(device_id):
    global dThingKey
    sens_data = {}
    thingKey = dThingKey[device_id]

    ##properties = ["SENS_LIGHT", "SENS_AIR_PRESSURE", "SENS_TEMPERATURE", "SENS_CARBON_DIOXIDE", "SENS_RELATIVE_HUMIDITY", "SENS_SOUND", "SENS_NITRIC_OXIDE", "SENS_ULTRA_VIOLET", "SENS_PM2P5", "SENS_PM10", "SENS_NITROGEN_DIOXIDE", "SENS_CARBON_MONOXIDE", "SENS_SULPHUR_DIOXIDE", "SENS_OZONE"]

    properties = http_items[device_id]["properties"]

    poll_url = http_items[device_id]["pollUrl"].replace("{thingKey}", thingKey)	    ##"http://52.28.187.167/services/api/v1/property/{thingKey}/{propertyKey}/1m"
    
    for p in properties:
        sleep(1)
        r = requests.get(url=poll_url.replace("{propertyKey}", p), headers = http_items[device_id]["thing_headers"])
	parser = JsonTraverseParser(r.json())
        if(parser.traverse(http_items[device_id]["getDataField"]) is not None):
	    sens_data.update({p:parser.traverse(http_items[device_id]["getDataField"])})	    ##getDataField = results.values.0.value
        else:
            sens_data.update({p:None})    
    try:
	data = {}
	data["data"] = json.dumps(sens_data)
	http_dict = {device_id:json.dumps(data)}
	print("Pushed", http_dict)
	redConn.rpush("incoming-messages", http_dict)
	decode_push.delay()
    except Exception as e:
	print("Couldn't add HTTP data")
	print(e)

#----------------------------------------------------------------------------------------------------#

# Defining various callbacks for MQTT and AMQP connections


def MWSub_onMessage(ch, method, properties, body):
    if '.rx' in method.routing_key:						## rx for device reception
        # Change according to wildcard entry
        _id = method.routing_key.replace('.rx', '')
        am_dict = {}
        am_dict = {_id:body.decode('utf-8')}
        redConn.rpush("outgoing-messages", am_dict)
        print("Received", am_dict)
        encode_push.delay()
    else:
	pass
#_____!!!!!!ADD A CHECK AND METHOD FOR HTTP DEVICES UP THERE, IN CASE IT IS IMPLEMENTED IN FUTURE!!!!!!_____#


def NSSub_onConnect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))


def NSSub_onMessage(client, userdata, msg):
    in_str = json.dumps(json.loads(msg.payload.decode('utf-8')))
    mq_parser = JsonTraverseParser(in_str)
    dev_id = msg.topic.split('/')[3]
    _str = mq_parser.traverse(items[dev_id]["getDataField"])
    mq_dict = {}
    mq_dict = {dev_id: _str}
    redConn.rpush("incoming-messages", mq_dict)
    print("Pushed", mq_dict)
    decode_push.delay()

#----------------------------------------------------------------------------------------------------#

# Initialization at startup


adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}

http_items = {}
items = {}

try:
    mres = mcln.find(projection={"_id": False})
    for obj in mres:
        items.update(obj)
except Exception as e:
    print("Couldn't load MQTT lists")
    print(e)

try:
    hres = hcln.find(projection={"_id": False})
    for obj in hres:
        http_items.update(obj)
except Exception as e:
    print("Couldn't load HTTP lists")
    print(e)

try:
    for item in list(items.keys()):
        try:
            modules[item] = {}
            from_spec = importlib.util.spec_from_file_location('from_' + item + '_pb2', adaptersDir + '/id_' + item + '/from_' + item + '_pb2.py')
            from_mod = importlib.util.module_from_spec(from_spec)
            from_spec.loader.exec_module(from_mod)
            modules[item]["protoFrom"] = getattr(from_mod, items[item]["protoFrom"])()

            to_spec = importlib.util.spec_from_file_location('to_' + item + '_pb2', adaptersDir + '/id_' + item + '/to_' + item + '_pb2.py')
            to_mod = importlib.util.module_from_spec(to_spec)
            to_spec.loader.exec_module(to_mod)
            modules[item]["protoTo"] = getattr(to_mod, items[item]["protoTo"])()

        except Exception as e:
            print("Couldn't load", item)
            print(e)
except Exception as e:
    print("Couldn't load")


try:
    for ids in list(http_items.keys()):
        try:
	    bosch_init(ids)
	except Exception as e:
	    print("Couldn't initialise API for ID", ids)
	    print(e)
	try:
            scheduler.add_job(func=poll_to_url.delay, args=[ids], trigger='interval', seconds=60)
            print("Added job for ID", ids)
        except Exception as e:
            print("Couldn't add process with ID", ids)
            print(e)

except Exception as e:
    print("Couldn't add processes")
    print(e)


try:
    scheduler.start()
    threading.Thread(asyncio.get_event_loop().run_forever()).start()
except Exception as e:
    print("Couldn't start scheduler")
    print(e)


def server():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    socket.bind("tcp://*:%s" % 1617)
    while True:
        message = socket.recv()
        print("Received request  %s" % message)
        itemEntry = json.loads(str(message, 'utf-8'))
        itemId = itemEntry["id"]
        
	if len(list(itemEntry.keys())) == 17:
	    http_items.update({itemId:itemEntry})
	    try:
		bosch_init(itemId)
	    except Exception as e:
		print("Couldn't initialise API for ID", itemId, "after registration")
		print(e)
            try:
                scheduler.add_job(pool_to_url(itemId), 'interval', seconds=60)
            except Exception as e:
                print("Couldn't add job for ID", itemId, "after registration")
		print(e)
	    
        else:
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

            except Exception as e:
                print("Couldn't load objects")
		print(e)


Process(target=server).start()

#----------------------------------------------------------------------------------------------------#

# Connection parameters for AMQP and MQTT connections

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
nsSubParams["topic"] = "application/1/node/+/tx"
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


if __name__ == "__main__":
    main()
