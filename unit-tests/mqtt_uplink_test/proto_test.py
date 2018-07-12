from google.protobuf import json_format
import importlib.machinery
import json
from random import randrange
import os
from time import sleep, time
from google.protobuf.json_format import MessageToDict

adaptersDir = os.getcwd() + "/adapters"
cwd = os.getcwd()

modules = {}

_id = '70b3d58ff0031de5'

items={"70b3d58ff0031de5": {"protoTo": "_targetConfigurations", "protoFrom": "sensor_values", "id": "70b3d58ff0031de5"}}

def timerfunc(func):
    """
    A timer decorator
    """
    def function_timer(*args, **kwargs):
        """
        A nested function for timing other functions
        """
        start = time()
        value = func(*args, **kwargs)
        end = time()
        runtime = end - start
        msg = "The runtime for {func} took {time} seconds to complete"
        print(msg.format(func=func.__name__,
                         time=runtime))
        return value
    return function_timer

try:
    item=_id
    modules[item] = {}
    from_spec = importlib.util.spec_from_file_location('from_' + item + '_pb2', adaptersDir + '/id_' + item + '/from_' + item + '_pb2.py')
    from_mod = importlib.util.module_from_spec(from_spec)
    from_spec.loader.exec_module(from_mod)
    modules[item]["protoFrom"] = getattr(from_mod, items[item]["protoFrom"])()

except Exception as e:
    print("Couldn't load")

@timerfunc
def decode(data):
    ns_sensor_message = modules[_id]["protoFrom"]
    b64en=data["data"].encode("utf-8")
    ns_sensor_message.ParseFromString(b64en)
    mw_message = MessageToDict(ns_sensor_message)

@timerfunc
def encode(body):
    jbody=json.dumps(json.loads(body))
    mw_actuation_message = modules[_id]["protoFrom"]
    data = {}
    data["reference"] = "a"
    data["confirmed"] = False
    data["fport"] = 1
    json_format.Parse(jbody, mw_actuation_message, ignore_unknown_fields=False)
    data["data"] = (mw_actuation_message.SerializeToString()).decode("utf-8")
    decode(data)


def main():
    body={}
    body["dataSamplingInstant"]=randrange(28)
    body["caseTemperature"]=randrange(28)
    body["powerConsumption"]=randrange(28)
    body["luxOutput"]=randrange(28)
    body["ambientLux"]=randrange(28)
    body["batteryLevel"]=randrange(28)
    body["slaveAlive"]=True
    jbody=json.dumps(body)
    
    encode(jbody)



if __name__=="__main__":
    main()
