
import zmq
import time
import sys
from  multiprocessing import Process

def client():

    context = zmq.Context()
    print("Connecting to server with ports %s" % 5555)
    socket = context.socket(zmq.REQ)
    
    socket.connect ("tcp://localhost:%s" % 5555)
    for request in range (20):
        print("Sending request ", request,"...")
        socket.send_string("Hello")
        message = socket.recv() 
        time.sleep (1) 

if __name__=="__main__":

    # Now we can connect a client to all these servers
    Process(target=client).start()
