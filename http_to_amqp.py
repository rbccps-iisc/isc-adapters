import asyncio
import requests
from AMQPPubSub import AMQPPubSub
from time import sleep
import redis, hiredis
from celery import Celery
import zmq
import json
import os
import base64
import sys
from multiprocessing import Process
from jsonschema import validate
import ast
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import adapter

redConn = redis.StrictRedis(host='localhost', port=6379, db=0)

scheduler = AsyncIOScheduler

#------------------------------------------------------------------------------------------------------------------------

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
##		schema(json=data['data'], devId= _id)
##		#------!!!ADD code for 'post'ing to HTTP SERVER!!!------

#-------------------------------------------------------------------------------------------------------------------------

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
		print('Given device data is not valid to its schema..')
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
			scheduler.add_job(pool_to_url(itemId), 'interval', seconds = 10)
        	except:
		print("Couldn't start polling to ID ", itemId)
		print(sys.exc_traceback.tb_lineno)

Process(target=server).start()

protosJson = {}

#----------------------------------------------------------------------------------------------

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

#-------------------------------------------------------------------------------------------------------------------------------

#Defining celery tasks for polling to URLs

poll=Celery('Poll', broker='redis://localhost/0')

@poll.task
def poll_to_url(device_id):
	while True:
		r=requests.get(""+device_id)					#------!!!ADD APPROPRIATE URL!!!------
		if r.status_code==requests.status.ok:
			http_dict=[device_id:r.text]					
			redConn.push("incoming-messages", http_dict)
			adapter.decode_push.delay()
			await asyncio.sleep(5)

#-------------------------------------------------------------------------------------------------------------------------------

def main():
	try:
		with open(cwd + '/items.json', 'r') as f:
			items = json.load(f)
	        	for item in items.keys(): 
		        	try:
					scheduler.add_job(poll_to_url.delay(item), 'interval', seconds = 10)
				except Exception as e:
					print("Couldn't add process with ID", item)
					print(e)
	except:
		print("Couldn't add processes")
	scheduler.start()
	asyncio.get_event_loop().run_forever()


if __name__=="__main__":
    main()
