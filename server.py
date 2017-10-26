import zmq
import time
import sys
from  multiprocessing import Process

def server():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % 5555)
    print("Running server on port: ", 5555)
    # serves only 5 request and dies
    for reqnum in range(5):
        # Wait for next request from client
        message = socket.recv()
        print("Received request #%s: %s" % (reqnum, message))
        socket.send_string("World from %s" % 5555)
         

if __name__ == "__main__":
    # Now we can run a few servers 
    Process(target=server).start()
    print("Non Blocking")
        
