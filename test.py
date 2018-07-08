import requests
import json
from time import sleep

properties = ["SENS_LIGHT", "SENS_AIR_PRESSURE", "SENS_TEMPERATURE", "SENS_CARBON_DIOXIDE",
"SENS_RELATIVE_HUMIDITY", "SENS_SOUND", "SENS_NITRIC_OXIDE", "SENS_ULTRA_VIOLET", "SENS_PM2P5",
"SENS_PM10", "SENS_NITROGEN_DIOXIDE", "SENS_CARBON_MONOXIDE", "SENS_SULPHUR_DIOXIDE", "SENS_OZONE"]


bosch_auth_url = "http://52.28.187.167/services/api/v1/users/login"
bosch_auth_headers = {"Content-Type":"application/json", "Accept":"application/json", "api_key":"apiKey"}
bosch_auth_payload = {"password":"Q2xpbW9AOTAz", "username":"SUlTQ19CQU5HQUxPUkU="}
auth_res = requests.post(url = bosch_auth_url, headers = bosch_auth_headers, data = json.dumps(bosch_auth_payload))

authToken = auth_res.json()["authToken"]
OrgKey = auth_res.json()["OrgKey"]


bosch_thing_url = "http://52.28.187.167/services/api/v1/getAllthings"
bosch_thing_headers = {"Accept":"application/json", "api_key":"apiKey", "Authorization":authToken, "X-OrganizationKey":OrgKey}
thing_res = requests.get(url = bosch_thing_url, headers = bosch_thing_headers)

thingKey = thing_res.json()["result"][0]["thingKey"]


poll_url = "http://52.28.187.167/services/api/v1/property/" + thingKey + "/{propertyKey}/1m"


sens_data = {}
for p in properties:
    sleep(1)
    r = requests.get(url=poll_url.replace("{propertyKey}", p), headers = bosch_thing_headers)
    print(p, ":", r.json()["result"]["values"])
    if(r.json()["result"]["values"] is not None):
        sens_data.update({p:r.json()["result"]["values"][0]["value"]})
    else:
        pass
    
data = {}
data["reference"] = "a"
data["confirmed"] = False
data["fport"] = 1
data["data"] = json.dumps(sens_data)
http_dict = json.dumps(data)
#print(sens_data)
