#!/usr/bin/python3

from flask import Flask, request 
from flask_restful import Resource, Api, reqparse
import json 
import requests
import os
import subprocess as sub
import sys
import rq 
from shutil import copyfile
import zmq


app = Flask(__name__)
api = Api(app)

objectListFile = "objectList.json"
objectList = []
itemEntry = {}

context = zmq.Context()
print("Connecting to Adapter with ports %s" % 5555)
socket = context.socket(zmq.REQ)
socket.setsockopt(zmq.LINGER, 2)
socket.connect ("tcp://localhost:%s" % 5555)
poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)

#
#   { "id" : 70b3d58ff01201, "protoTo" : "msgName", "protoFrom":"msgName" }
#

workingDir = sys.path[0]


cwd = os.getcwd()

catURL = ""
protoURL = ""

class Register(Resource):
    def post(self):
        flag = 0
        try:
            json_data = request.get_json()
            catURL = json_data["catURL"]
            catJSON = requests.get(catURL).json()
            id = catJSON["items"][0]["id"]
            itemEntry["id"] = id
            adapterRoot = cwd + '/adapters/id_' + id
            os.mkdir(adapterRoot, mode = 0o755)
            sys.path.insert(0, workingDir + '/' + itemEntry["id"])
            
            try:
                protoTo = catJSON["items"][0]["serialization_to_device"]["schema_ref"]
                protoToLink = protoTo["link"]
                with open(adapterRoot + '/to.proto','wb') as file:
                    resp = requests.get(protoToLink)
                    file.write(resp.content)
                p = sub.call('protoc -I=' + adapterRoot + ' --python_out=' + adapterRoot + ' ' +  adapterRoot + '/to.proto'  ,shell=True)
                itemEntry["protoTo"] = catJSON["items"][0]["serialization_to_device"]["schema_ref"]["mainMessageName"]
                flag = flag + 1 
            except:
                print("Couldn't get *To* Proto")


            try:
                protoFrom = catJSON["items"][0]["serialization_from_device"]["schema_ref"]
                protoFromLink = protoFrom["link"]
                with open(adapterRoot + '/from.proto','wb') as file:
                    resp = requests.get(protoFromLink)
                    file.write(resp.content)
                                
                p = sub.call('protoc -I=' + adapterRoot + ' --python_out=' + adapterRoot + ' ' +  adapterRoot + '/from.proto'  ,shell=True)
                itemEntry["protoFrom"] = catJSON["items"][0]["serialization_from_device"]["schema_ref"]["mainMessageName"]
                flag = flag + 1
            except:
                print("Couldn't get *From* Proto")
            
            copyfile('initstructure.tmpl', adapterRoot + '/__init__.py')

            objectList.append(itemEntry)
            print(objectList)          
            with open(objectListFile, 'w') as jsFile:
                json.dump(objectList, jsFile)

            if( flag == 2):
                flag = 0
                socket.send_string(json.dumps(itemEntry))
                if poller.poll(10*1000): # 10s timeout in milliseconds
                    msg = socket.recv()
                    return "Success"
                else:
                    return "Failure"

        except Exception as e:
            print(e)

            

	
api.add_resource(Register, '/register')

if __name__ == '__main__':
    app.run(debug=True)
