from MQTTPubSub import MQTTPubSub
from AMQPPubSub import AMQPPubSub 
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
import time
import sys
import json
import base64
from multiprocessing import Process
import zmq
import os
import importlib.machinery


adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}
items = {}


ns_rx_topic = "application/1/node/{id}/rx"
ns_tx_topic = "application/1/node/{id}/tx"

#sahil
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



Process(target=server).start()


protosJson = {}




def NSSub_onMessage(mqttc, obj, msg):
       

# NS Message topics are of the form application/{applicationId}/node/{id}/rx
    try:
        try:
            topic = msg.topic.split('/') 
            itemId = topic[3] #{id} is the 4th field
            print('Received ', itemId, ' from NS')
        except Exception as e:
            print("ignored", topic)
        if itemId in modules:
            ns_sensor_message = modules[itemId]["protoFrom"] 
            jsonData = json.loads((msg.payload).decode("utf-8"))
            decodedData = base64.b64decode(jsonData["data"])
            ns_sensor_message.ParseFromString(decodedData)
            mw_message = MessageToDict(ns_sensor_message) 
            print (mw_message)
            mwSub.publish(itemId,json.dumps(mw_message))
    except Exception as e:
        print("DECODE ERROR")
        print(e)





def MWSub_onMessage(ch, method, properties, body):

#Change according to wildcard entry 
    try:
    
        if("_update" in method.routing_key  ): 
            _id = method.routing_key.replace('_update','')
            if( _id in modules):
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

                # edit by sahil
                schema(json=data['data'], devId=_id)
                if (validationFlag):
                    nsSub.publish(ns_tx_topic.replace("{id}", _id), json.dumps(data))

        else:
            print("Ignored", method.routing_key)
    except Exception as e:
        print("DECODE ERROR")
        print(e)



def NSSub_onConnect(client, userdata, flags, rc):
    
    print("Connected to NS SUB result code "+str(rc))



def NSPub_onConnect(client, userdata, flags, rc):
    
    print("Connected to NS PUB with result code "+str(rc))







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







def main():
    mwSub_rc = mwSub.run()
    nsSub_rc = nsSub.run()
	
	

    while True:
        time.sleep(10)




if __name__ == "__main__":
    main()
