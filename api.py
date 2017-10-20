#!/usr/bin/python3

from flask import Flask, request 
from flask_restful import Resource, Api, reqparse
import json 
import requests
import os
import subprocess as sub

app = Flask(__name__)
api = Api(app)



#parser = reqparse.RequestParser()
#parser.add_argument('catURL', type=string, help='catURL not provided')
#parser.add_argument('protoURL', type=string, help='protoURL not provided')
#args = parser.parse_args()

cwd = os.getcwd()
protoLink = "https://raw.githubusercontent.com/rbccps-iisc/applications-streetlight/master/proto_stm/txmsg/sensed.proto"

catURL = ""
protoURL = ""

class Register(Resource):
    def post(self):
        
        try:
            json_data = request.get_json()
            catURL = json_data["catURL"]
            protoURL = json_data["protoURL"]
            catJSON = requests.get(catURL).json()
            id = catJSON["items"][0]["id"]
            adapterRoot = cwd + '/adapters/' + id
            os.mkdir(adapterRoot, mode = 0o755)
            protoRoot = adapterRoot + '/protos'
            os.mkdir(protoRoot, mode = 0o755)
            protoFrom = catJSON["items"][0]["serialization_from_device"]
            protoTo =  catJSON["items"][0]["serialization_to_device"]

            with open(protoRoot + '/from.proto','wb') as file:
                resp = requests.get(protoLink)
                file.write(resp.content)


            #ADD to.proto code here
            #
            ##

            p = sub.call('protoc -I=' + protoRoot + ' --python_out=' + protoRoot + ' ' +  protoRoot + '/from.proto',shell=True)

        except Exception as e:
            print(e)

            

	
api.add_resource(Register, '/register')

if __name__ == '__main__':
    app.run(debug=True)
