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

redConn = redis.StrictRedis(host='localhost', port=6379, db=0)

scheduler = AsyncIOScheduler()

poll_url = "" < SPECIFY MASTER URL FOR POLLING HERE >

#----------------------------------------------------------------------------------------------------#

# MongoDB setup

client = pymongo.MongoClient()

mdb = client.devices_db_mq	    # DB OF DEVICES
mcln = mdb.devices		    # COLLECTION OF DEVICES REGISTERED

hdb = client.devices_db_http
hcln = hdb.devices

#----------------------------------------------------------------------------------------------------#

# Code for celery tasks

celery_app = Celery('Adapter', broker='redis://localhost/0')


@celery_app.task
def decode_push():
    global red
    mes_pay = redConn.lpop("incoming-messages")
    mes_dict = ast.literal_eval(mes_pay.decode('utf-8'))
    dec_payload = list(mes_dict.values())[0]
    dec_device_id = list(mes_dict.keys())[0]
    jsonData = json.loads(dec_payload)
    if dec_device_id in http_items:
        b64en = jsonData["data"]
        decodedData = base64.b64decode(b64en.encode('utf-8'))
        mwSub.publish(dec_device_id+"_tx",
                      json.dumps(decodedData.decode('utf-8')))
    elif dec_device_id in modules:
        # code for proto decode
        try:
            b64en = jsonData["data"]
            decodedData = base64.b64decode(b64en.encode('utf-8'))
            ns_sensor_message = modules[dec_device_id]["protoFrom"]
            ns_sensor_message.ParseFromString(decodedData)
            mw_message = MessageToDict(ns_sensor_message)
            mwSub.publish(dec_device_id+'_tx', json.dumps(mw_message))
            print("Published", mw_message)
        except Exception as e:
            print("DECODE ERROR")
            print(e)


@celery_app.task
def encode_push():
    global red
    out_dict = ast.literal_eval(redConn.lpop("outgoing-messages").decode('utf-8'))
    en_body = (list(out_dict.values())[0])
    en_id = list(out_dict.keys())[0]
    try:
        mw_actuation_message = modules[en_id]["protoTo"]
        data = {}
        data["reference"] = "a"
        data["confirmed"] = False
        data["fport"] = 1
        print(en_body)
        json_format.Parse(en_body, mw_actuation_message, ignore_unknown_fields=False)
        data["data"] = base64.b64encode(mw_actuation_message.SerializeToString()).decode('utf-8')
        nsSub.publish(ns_rx_topic.replace("{id}", _id), json.dumps(data))
    except Exception as e:
        print("ENCODE ERROR")
        print(e)


@celery_app.task
def poll_to_url(device_id):
    r = requests.get("http://"+poll_url+"/"+device_id)
    if r.json is not None:
        data = {}
        data["reference"] = "a"
        data["confirmed"] = False
        data["fport"] = 1
        data["data"] = r.text
        print("GOT", data["data"])
        http_dict = {device_id: json.dumps(data)}
        print("Pushed", http_dict)
        redConn.rpush("incoming-messages", http_dict)
        decode_push.delay()

#----------------------------------------------------------------------------------------------------#

# Defining various callbacks for MQTT and AMQP connections


def MWSub_onMessage(ch, method, properties, body):
    if '_rx' in method.routing_key:
        # Change according to wildcard entry
        d_id = method.routing_key.replace('_update', '')
        _id = d_id[:len(d_id)-3]
        am_dict = {}
        am_dict = {_id: body.decode('utf-8')}
        redConn.rpush("outgoing-messages", am_dict)
        print("Received", am_dict)
        encode_push.delay()
#_____!!!!!!ADD A CHECK AND METHOD FOR HTTP DEVICES UP THERE, IN CASE IT IS IMPLEMENTED IN FUTURE!!!!!!_____#


def NSSub_onConnect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))


def NSSub_onMessage(client, userdata, msg):
    global r
    in_str = json.dumps(json.loads(msg.payload.decode('utf-8')))
    mq_dict = {}
    mq_dict = {msg.topic.split('/')[3]: in_str}
    redConn.rpush("incoming-messages", mq_dict)
    print("Pushed", mq_dict)
    decode_push.delay()

#----------------------------------------------------------------------------------------------------#

# To import proto files to the code at startup


adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}
http_items = []
items = {}

ns_rx_topic = "application/1/node/{id}/rx"  # Messages from middleware
ns_tx_topic = "application/1/node/{id}/tx"  # Messages to middleware

try:
    mres = mcln.find(projection={'_id': False})
    for ids in mres:
        items.update(ids)

    hres = hcln.find(projection={'_id': False})
    for ids in hres:
        http_items.append(ids["id"])

    for item in list(items.keys()):
        try:
            modules[item] = {}
            from_spec = importlib.util.spec_from_file_location('from_' + item + '_pb2', adaptersDir + '/id_' + item + '/from_' + item + '_pb2.py')
            from_mod = importlib.util.module_from_spec(from_spec)
            from_spec.loader.exec_module(from_mod)
            modules[item]["protoFrom"] = getattr(
                from_mod, items[item]["protoFrom"])()

            to_spec = importlib.util.spec_from_file_location('to_' + item + '_pb2', adaptersDir + '/id_' + item + '/to_' + item + '_pb2.py')
            to_mod = importlib.util.module_from_spec(to_spec)
            to_spec.loader.exec_module(to_mod)
            modules[item]["protoTo"] = getattr(
                to_mod, items[item]["protoTo"])()

            # CONFIRM CODE HERE WITH FILE NAMES AND FILE TYPES
            api_spec = importlib.utli.spec_from_file_location('server_' + item, adaptersDir + '/id_' + item + '/server' + item + )  # COMPLETE THE ADDRESS HERE
            api_mod = importlib.util.module_from_spec(api_spec)
            api_spec.loader.exec_module(api_mod)
            modules[item]["serverName"] = getattr(api_mod, items[item]["serverName"])()
            modules[item]["getAddress"] = getattr(api_mod, items[item]["get"])()
            modules[item]["postAddress"] = getattr(api_mod, items[item]["post"])()
        except Exception as e:
            print("Couldn't load", item)
            print(e)
except Exception as e:
    print("Couldn't load")


try:
    for devID in http_items:
        try:
            scheduler.add_job(func=poll_to_url.delay, args=[devID], trigger='interval', seconds=10)
            print("Added job for ID", devID)
        except Exception as e:
            print("Couldn't add process with ID", devID)
            print(e)

# CORRECT FILE NAME AND TYPE HERE
        api_spec = importlib.utli.spec_from_file_location('server_' + item, adaptersDir + '/id_' + item + '/server' + item + )  # COMPLETE THE ADDRESS HERE
        api_mod = importlib.util.module_from_spec(api_spec)
        api_spec.loader.exec_module(api_mod)
        modules[item]["serverName"] = getattr(api_mod, items[item]["serverName"])()
        modules[item]["getAddress"] = getattr(api_mod, items[item]["get"])()
        modules[item]["postAddress"] = getattr(api_mod, items[item]["post"])()

except Exception as e:
    print("Couldn't add processes")
    print(e)


try:
    scheduler.start()
    threading.Thread(asyncio.get_event_loop().run_forever()).start()
except Exception as e:
    print("Couldn't add")
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
        if len(list(itemEntry.keys())) == 1:
            http_items.append(itemId)
            try:
                scheduler.add_job(pool_to_url(itemId), 'interval', seconds=10)
            except:
                print("Couldn't add job for ID", itemId, "after registration")
        else:
            modules[itemId] = {}
            try:
                from_spec = importlib.util.spec_from_file_location(
                    'from_' + itemId + '_pb2', adaptersDir + '/id_' + itemId + '/from_' + itemId + '_pb2.py')
                from_mod = importlib.util.module_from_spec(from_spec)
                from_spec.loader.exec_module(from_mod)
                modules[itemId]["protoFrom"] = getattr(from_mod, itemEntry["protoFrom"])()

                to_spec = importlib.util.spec_from_file_location('to_' + itemId + '_pb2', adaptersDir + '/id_' + itemId + '/to_' + itemId + '_pb2.py')
                to_mod = importlib.util.module_from_spec(to_spec)
                to_spec.loader.exec_module(to_mod)
                modules[itemId]["protoTo"] = getattr(to_mod, itemEntry["protoTo"])()

                # CORRECT FILE NAME AND TYPE
                api_spec = importlib.utli.spec_from_file_location('server_' + item, adaptersDir + '/id_' + item + '/server' + item + )  # COMPLETE THE ADDRESS HERE
                api_mod = importlib.util.module_from_spec(api_spec)
                api_spec.loader.exec_module(api_mod)
                modules[item]["serverName"] = getattr(api_mod, items[item]["serverName"])()
                modules[item]["getAddress"] = getattr(api_mod, items[item]["get"])()
                modules[item]["postAddress"] = getattr(api_mod, items[item]["post"])()
            except:
                print("Couldn't load objects")


Process(target=server).start()

protosJson = {}

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


if __name__ == "__main__":
    main()
