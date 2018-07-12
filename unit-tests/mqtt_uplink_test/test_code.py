import os
import paho.mqtt.client as mqtt
import base64
from google.protobuf import json_format
import importlib.machinery
import json
from random import randrange
from pymongo import MongoClient
from time import sleep

#-------------------------------------------------------------------------------------

mclient=MongoClient()

mdb=mclient.devices_db

mcln=mdb.devices

mcln.delete_many({})

#-------------------------------------------------------------------------------------

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

#-------------------------------------------------------------------------------------

_id = '70b3d58ff0031de5'

items={"70b3d58ff0031de5": {"protoTo": "_targetConfigurations", "protoFrom": "sensor_values", "id": "70b3d58ff0031de5"}}

mcln.insert_one(items)

#-------------------------------------------------------------------------------------

adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}
try:
    item=_id
    modules[item] = {}
    from_spec = importlib.util.spec_from_file_location('from_' + item + '_pb2', adaptersDir + '/id_' + item + '/from_' + item + '_pb2.py')
    from_mod = importlib.util.module_from_spec(from_spec)
    from_spec.loader.exec_module(from_mod)
    modules[item]["protoFrom"] = getattr(from_mod, items[item]["protoFrom"])()
except Exception as e:
    print("Couldn't load")

#------------------------------------------------------------------------------------------------------------------------------------------------

client = mqtt.Client()
client.on_connect = on_connect

client.connect("localhost", 1883, 60)

ns_rx_topic = "application/1/node/{id}/rx"


for i in range(1000):
    body={}
    body["dataSamplingInstant"]=randrange(28)
    body["caseTemperature"]=randrange(28)
    body["powerConsumption"]=randrange(28)
    body["luxOutput"]=randrange(28)
    body["ambientLux"]=randrange(28)
    body["batteryLevel"]=randrange(28)
    body["slaveAlive"]=True

    jbody=json.dumps(body)
    print(jbody)

    mw_actuation_message = modules[_id]["protoFrom"]
    data = {}
    data["reference"] = "a"
    data["confirmed"] = False
    data["fport"] = 1
    json_format.Parse(jbody, mw_actuation_message, ignore_unknown_fields=False)
    data["data"] = (base64.b64encode(mw_actuation_message.SerializeToString())).decode("utf-8")
    client.publish(ns_rx_topic.replace("{id}", _id), json.dumps(data))
    print(json.dumps(data))
    sleep(1/10)


