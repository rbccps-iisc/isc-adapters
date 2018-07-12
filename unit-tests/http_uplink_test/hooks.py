import redis
import celery
from time import sleep
import subprocess as sub

def check_adapter(*args, **kwargs):
	time.sleep(.5)  # give it a chance to start
	r = redis.StrictRedis(host='localhost', port=6379, db=0)
	r.set('foo', 'bar')
	if(r.get('foo') == 'bar'):
		cel=sub.call("celery -A adapter status", shell = True)
		if(cel==0):
			mon=sub.call("service mongodb status", shell = True)
			if(mon==0):
				return True
			else:
				return False
		else:
			return False
	else:
		return False

def check_api(*args, **kwargs):
	time.sleep(.5)  # give it a chance to start
	mon=sub.call("service mongodb status", shell = True)
	if(mon==0):
		return True
	else:
		return False
