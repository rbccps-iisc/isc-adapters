from .MQTTPubSub import MQTTPubSub
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict
import time
import sys
import json
sys.path.append("applicationProtos")
from .applicationProtos import sensed_pb2
from .applicationProtos import actuated_pb2
import base64


protosJson = {}




def NSSub_onMessage(mqttc, obj, msg):
	

	ns_sensor_message = sensed_pb2.sensor_values()
	jsonData = json.loads((msg.payload).decode("utf-8"))
	decodedData = base64.b64decode(jsonData["data"])
	ns_sensor_message.ParseFromString(decodedData)
	mw_message = MessageToDict(ns_sensor_message) 
	print (mw_message)
	mwPub.publish(json.dumps(mw_message))






def MWSub_onMessage(mqttc, obj, msg):
    
	mw_actuation_message = actuated_pb2.targetConfigurations()
	data = {}
	data['reference'] = 'a'
	data['confirmed'] = False
	data['fport'] = 1
	print (msg.payload)
	json_format.Parse(msg.payload, mw_actuation_message, ignore_unknown_fields=False)
	data['data'] = (base64.b64encode(mw_actuation_message.SerializeToString())).decode("utf-8")
	nsPub.publish(json.dumps(data))
    


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
mwSubParams["topic"] = "70b3d58ff0031de5_update"
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
mwPubParams["topic"] = "70b3d58ff0031de5"
mwPubParams["username"] = "admin"
mwPubParams["password"] = "admin@123"
#mwPubParams["onMessage"] = MWPub_onMessage
mwPub = MQTTPubSub(mwPubParams)







nsSubParams = {}
nsSubParams["url"] = "10.156.14.16"
nsSubParams["port"] = 1883
nsSubParams["timeout"] = 60
nsSubParams["topic"] = "application/2/node/70b3d58ff0031de5/rx"
nsSubParams["onMessage"] = NSSub_onMessage
nsSubParams["onConnect"] = NSSub_onConnect
nsSubParams["username"] = "loraserver"
nsSubParams["password"] = "password"
nsSub = MQTTPubSub(nsSubParams)


nsPubParams = {}
nsPubParams["url"] = "10.156.14.16"
nsPubParams["port"] = 1883
nsPubParams["timeout"] = 60
nsPubParams["topic"] = "application/2/node/70b3d58ff0031de5/tx"
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
