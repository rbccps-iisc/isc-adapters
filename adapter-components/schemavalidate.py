import pika
import urllib.request,json
import sys
sys.path.append('applicationProtos')
from .applicationProtos import sensed_pb2
from .applicationProtos import actuated_pb2
import base64
from google.protobuf.json_format import MessageToDict
from google.protobuf import json_format

def subscribeToBroker(user, passwd, host, port, vHost, exchange, queue):
    credentials = pika.PlainCredentials(user, passwd)
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port, virtual_host=vHost, credentials=credentials))
    print('connection to amqp established..')

    channel = conn.channel()
    channel.exchange_declare(exchange=exchange)
    channel.queue_bind(exchange=exchange, queue=queue)

    def callback(ch, method, properties, body):
        # if 'json' do normal validation, if 'protocol-buffers' do a proto conversion, convert to json and finally validate
        print("[x]", body)
        # sensedProto(body)

    channel.basic_consume(callback, queue=queue)
    channel.start_consuming()

#json parser on catalogue server
with urllib.request.urlopen('https://smartcity.rbccps.org/api/0.1.0/cat') as resp:
    data = json.loads(resp.read().decode())
    sensor_data = data['items']
    data.clear()

serialFromStatus={}
serialToStatus={}
deviceFrom_format={}
deviceTo_format={}
sensor_schema={}
for i in range(0,len(sensor_data)):
    serialFromStatus[sensor_data[i]['id']] = sensor_data[i]['serialization_from_device']['schema_ref']['link']
    deviceFrom_format[sensor_data[i]['id']] = sensor_data[i]['serialization_from_device']['format']
    sensor_schema[sensor_data[i]['id']] = sensor_data[i]['data_schema']
    if 'serialization_to_device' in sensor_data[i]:
        serialToStatus[sensor_data[i]['id']] = sensor_data[i]['serialization_to_device']['schema_ref']['link']
        deviceTo_format[sensor_data[i]['id']] = sensor_data[i]['serialization_to_device']['format']

def sensedProto(body):
    proto_sensed = sensed_pb2.sensor_values()
    sensed_msg = json.loads(body.decode('utf-8'))
    decode_sensed = base64.b64decode(sensed_msg['data'])
    proto_sensed.ParseFromString(decode_sensed)
    msg_sensed = MessageToDict(proto_sensed)
    print(msg_sensed)

def actuatedProto(body):
    proto_actuated = actuated_pb2.targetConfigurations()
    actuated ={}
    actuated['reference'] = 'a'
    actuated['confirmed'] = False
    actuated['fport'] = 1
    json_format.Parse(body, proto_actuated, ignore_unknown_fields=False)
    actuated['data'] = (base64.b64encode(proto_actuated.SerializeToString())).decode("utf-8")
    print(actuated)

#subscribeToBroker(host='10.156.14.6',user='rbccps',passwd='rbccps@123',port=5672,vHost='/',exchange='amqp.topic',queue='database_queue')