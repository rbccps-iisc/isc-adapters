import asyncio
import requests
from AMQPPubSub import AMQPPubSub
from time import sleep
import redis, hiredis
from celery import Celery
import zmq
import json
import base64
from multiprocessing import Process
import ast
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import adapter
import pymongo

redConn = redis.StrictRedis(host='localhost', port=6379, db=0)

scheduler = AsyncIOScheduler()

#----------------------------------------------------------------------------------------------------------------------#

#MongoDB setup

client=pymongo.MongoClient()

mdb=client.devices_db_mq                                #DB OF DEVICES

mcln=mdb.devices                                        #COLLECTION OF DEVICES REGISTERED

hdb=client.devices_db_http

hcln=hdb.devices

#----------------------------------------------------------------------------------------------------------------------#

###Defining callbacks for AMQP connections
##
##def MWSub_onMessage(ch, method, properties, body):
##
##	#Change according to wildcard entry 
##	try:
##		_id = method.routing_key.replace('_update','')
##		print(_id)
##		print('Received ', _id, ' from MW')
##		data = {}
##		data['reference'] = 'a'
##		data['confirmed'] = False
##		data['fport'] = 1
##		print(body)
##		data['data'] = (base64.b64encode(body)).decode("utf-8")
##		#------!!!ADD code for 'post'ing to HTTP SERVER!!!------

#---------------------------------------------------------------------------------------------------------------------#

http_items=[]

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
		http_items.append(itemId)
		try:
			scheduler.add_job(pool_to_url(itemId), 'interval', seconds = 10)
		except:
			print("Couldn't add job for ID ", itemId)

Process(target=server).start()

protosJson = {}

#----------------------------------------------------------------------------------------------------------------------#

#Connection parameters for AMQP connection

mwSubParams = {}
mwSubParams["url"] = "10.156.14.6"
mwSubParams["port"] = 5672
mwSubParams["timeout"] = 60
#mwSubParams["onMessage"] = MWSub_onMessage
mwSubParams["username"] = "admin"
mwSubParams["password"] = "admin@123"
mwSubParams["exchange"] = "amq.topic"
mwSub = AMQPPubSub(mwSubParams)

#----------------------------------------------------------------------------------------------------------------------#

#Defining celery tasks for polling to URLs

poll=Celery('Poll', broker='redis://localhost/0')

@poll.task
def poll_to_url(device_id):
	while True:
		r=requests.get("http://"+poll_url+"/"+device_id)					#------!!!ADD APPROPRIATE URL!!!------
		if r.status_code==requests.codes.ok:
			data = {}
			data["reference"] = "a"
			data["confirmed"] = False
			data["fport"] = 1
			data["data"] = r.text					
			http_dict=[device_id:json.dumps(data)]
			redConn.push("incoming-messages", http_dict)
			adapter.decode_push.delay()

#-----------------------------------------------------------------------------------------------------------------------#

def main():
	poll_url = localhost
	mwSub_rc = mwSub.run()
	try:
		hres=hcln.find(projection={"id": True, "_id":False})
		for ids in resu:
			http_items=http_items+list(ids.values())
		for devID in http_items:
			try:
				scheduler.add_job(poll_to_url.delay(devID), 'interval', seconds = 10)
			except Exception as e:
				print("Couldn't add process with ID", devID)
				print(e)
	except:
		print("Couldn't add processes")
	scheduler.start()
	asyncio.get_event_loop().run_forever()

if __name__=="__main__":
	main()
