from AMQPPubSub import AMQPPubSub
import time
params = {}

params['url'] = '10.156.14.6'
params['port'] = 5672
params['timeout'] = 60
params['exchange'] = 'amq.topic'
params['username'] = 'admin'
params['password'] = 'admin@123'


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))

params['onMessage'] = callback

def main():
    nw_amqp = AMQPPubSub(params)
    nw_amqp.run()


    while True:
        print('Waiting...')
        time.sleep(10)   

if __name__ == "__main__":
    main()

