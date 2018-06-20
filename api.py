from flask import Flask, request
from flask_restful import Resource, Api, reqparse
import json
import requests
import os
import subprocess as sub
import sys
from shutil import copyfile
import zmq
import pymongo

app = Flask(__name__)
api = Api(app)

client=pymongo.MongoClient()
db=client.devices_db                                    #DB OF DEVICES
cln=db.devices                                          #COLLECTION OF DEVICES REGISTERED


workingDir = sys.path[0]
items = {}

#LOAD JSON OBJECTS
try:
    #f = open(workingDir + "/items.json", 'r')
    #items = json.load(f)
    res=cln.find(projections={'_id':FALSE})
    for ids in res:
        items.update(ids)
except:
    print("Couldn't load list")

itemEntry = {}

context = zmq.Context()
print("Connecting to Adapter with ports %s" % 1617)
socket = context.socket(zmq.PUB)
socket.connect ("tcp://localhost:%s" % 1617)

#
#   { "70b3d58ff01201":{ "protoTo" : "msgName", "protoFrom":"msgName" }}
#



catURL = ""
protoURL = ""

class Register(Resource):
    def post(self):
        flag = 0
        try:
            json_data = request.get_json()
            catURL = json_data["catURL"]
            catJSON = requests.get(catURL, verify=False).json()
            id = catJSON["items"][0]["id"]
            adapterRoot = workingDir + '/adapters/id_' + id
            os.mkdir(adapterRoot, mode = 0o755)

            try:
                protoTo = catJSON["items"][0]["serialization_to_device"]["schema_ref"]
                protoToLink = protoTo["link"]
                with open(adapterRoot + '/to_' + id + '.proto','wb') as file:
                    resp = requests.get(protoToLink)
                    file.write(resp.content)
                p = sub.call('protoc -I=' + adapterRoot + ' --python_out=' + adapterRoot + ' ' +  adapterRoot + '/to_' + id + '.proto'  ,shell=True)
                itemEntry["protoTo"] = catJSON["items"][0]["serialization_to_device"]["schema_ref"]["mainMessageName"]
                flag = flag + 1
            except:
                print("Couldn't get *To* Proto")


            try:
                protoFrom = catJSON["items"][0]["serialization_from_device"]["schema_ref"]
                protoFromLink = protoFrom["link"]
                with open(adapterRoot + '/from_' + id + '.proto','wb') as file:
                    resp = requests.get(protoFromLink)
                    file.write(resp.content)

                p = sub.call('protoc -I=' + adapterRoot + ' --python_out=' + adapterRoot + ' ' +  adapterRoot + '/from_' + id + '.proto'  ,shell=True)
                itemEntry["protoFrom"] = catJSON["items"][0]["serialization_from_device"]["schema_ref"]["mainMessageName"]
                flag = flag + 1
            except:
                print("Couldn't get *From* Proto")


            items[id]=itemEntry
            itemEntry["id"] = id
            print(itemEntry)
##            with open(workingDir + '/items.json', 'w') as jsFile:
##                json.dump(items, jsFile)				#WRITE TO items.json
            cln.insert_one(items)
            if( flag == 2):
                flag = 0
                socket.send_string(json.dumps(itemEntry))

        except Exception as e:
            print(e)

api.add_resource(Register, '/register')

def main():
   app.run(debug=True)

if __name__=="__main__":
   main()
