import pika
import uuid
from flask import Flask, render_template, jsonify, request,abort
import docker
import os
import tarfile
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from random import randint
import atexit
from kazoo.client import KazooClient
from io import BytesIO

#zk = KazooClient(hosts='127.0.0.1:2181')
zk = KazooClient(hosts='zoo:2182')
zk.start()
FIRSTFLAG = False

if not zk.exists("/workers"):
    zk.create("/workers", b'HEAD SLAVE')

if not zk.exists("/election"):
    zk.create("/election", b'ELECTION')


@zk.ChildrenWatch('/workers', send_event=True)
def watch_children(children, event):
    #print("THE WATCH IS HERE...")
    global FIRSTFLAG
    if not FIRSTFLAG:
        FIRSTFLAG =True
        print("FLAG -----------------")
        return
    print("THE STALKER IS HERE >:)", "\n"*10)
    print("EVENT:", event, event.type)
    print(dir(event))
    print("CHILDREN LENGTH:", len(children))
    WIDTH = 20
    global global_count
    requiredSlaves = global_count
    print("req slaves = ",requiredSlaves)
    if(len(children)< requiredSlaves):
        FIRSTFLAG = False
        createSlave()

def getMaster():
	client = docker.from_env()
	pid_list = []
	for c in client.containers.list():
		if "slave" in c.name:
			pid = c.top()["Processes"][0][1]
			pid_list.append((pid, c))
	pid_list.sort(key=lambda x:x[0])
	return pid_list[0][1]


def getRequestCount():
    global THE_COUNT
    return THE_COUNT


def resetRequestCount():
    global THE_COUNT
    THE_COUNT = 0

def checkRequestCount():
    WIDTH = 20
    print("CHECKING COUNT...", "\n"*10)
    count = getRequestCount()
    print("Count:", count)
    count = (abs(int(count)-1) // WIDTH)+1
    global global_count
    global_count = count
    slaveCount = getNoOfSlaves()
    if slaveCount < 0:
        print("NEGETIVE COUNT!!!!!")
    diff = count - slaveCount
    if diff > 0:
        for i in range(diff):
            global FIRSTFLAG
            FIRSTFLAG = False
            createSlave()
    elif diff < 0:
        for i in range(abs(diff)):
            deleteSlave()
    else:
        print("NO change")

    resetRequestCount()



cron = BackgroundScheduler(daemon=True)
cron.add_job(checkRequestCount,'interval',seconds=120)


client = docker.from_env()
countFile = "myCount.txt"

timerStart = False


def startTimer():
    t = threading.Timer(1, checkRequestCount)
    t.start()

def slaveName():
    name = ""
    flag1 = False
    while not flag1:
        name = "slave"+str(randint(1, 10**3))
        flag2 = False
        for c in client.containers.list():
            if c.name == name:
                flag2 = True
                break
        if not flag2:
            flag1 = True
    print("SLAVE NAME:", name)
    return name


def getNoOfSlaves():
    client = docker.from_env()
    count = 0
    for c in client.containers.list():
        if "slave" in c.name:
            count += 1
    return count-1 ########################## -1 as one of them is master


def deleteSlave():
    client = docker.from_env()
    for c in client.containers.list():
        if "slave" in c.name and c.name != "slave1":
            c.stop()
            break
    print("SLAVE DELETED\n"*10)


def createSlave():
    c_name = slaveName()
    #c_name = c_name['name']
    client = docker.from_env()
    '''for c in client.containers.list():
        if c.name=='slave1':
            image_1 = c.image'''
    image_1 = client.images.get("theslave")
    print("GOT IMAGE")
    c1 = client.containers.run(image = image_1, command='python /code/slave.py', links = {'rmq':'rmq', 'zoo':'zoo'},detach = True ,
        network = 'testcloudcomputing_default', name=c_name, volumes={'/var/run/docker.sock': {'bind': '/var/run/docker.sock', 'mode': 'rw'},
        '/usr/bin/docker': {'bind': '/usr/bin/docker', 'mode': 'rw'}})

    print("GOT CONTAINER")
    copy_master_db_to_new_slave(c_name)
    print("SLAVE CREATED\n"*10)


def initCount():
    global THE_COUNT
    THE_COUNT = 0


def addCount():
    global THE_COUNT
    THE_COUNT += 1


def copy_master_db_to_new_slave(name):
    cl = docker.from_env()
    cont = getMaster()
    stream, status = cont.get_archive("/code/database/RideShare.db")
    file_obj = BytesIO()
    for i in stream:
        file_obj.write(i)
    file_obj.seek(0)
    tar = tarfile.open(mode='r', fileobj=file_obj)
    text = tar.extractfile('RideShare.db')
    q = text.read()
    #print(q)
    f = open("/code/database/RideShare.db", "wb")
    f.write(q)
    f.close()

    dst = '/code/database/RideShare.db' 
    src = '/code/database/RideShare.db'
    container = client.containers.get(name)
    print("GOT CREATED CONTAINER")
    os.chdir(os.path.dirname(src))
    srcname = os.path.basename(src)
    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(srcname)
    finally:
        tar.close()

    data = open(src + '.tar', 'rb').read()
    print("PUTTING ARCHIVE")
    container.put_archive(os.path.dirname(dst), data)
    print("PUT ARCHIVE")




class MyRPC(object):

    def __init__(self, type):
        self.rpc_type = type
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rmq',heartbeat = 0))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, details):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=self.rpc_type+'Q',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
		content_type = "application/json",
            ),
            body=str(details))
        while(self.response is None):
            self.connection.process_data_events()
        return (self.response)

write_rpc = MyRPC("write")
read_rpc = MyRPC("read")
app=Flask(__name__)

print(" [x] Request Sent")
@app.route("/")
def greet():
	return "Hi there!"

@app.route("/api/v1/db/write", methods=["POST"])
def write_deb():
    '''global cron
    global timerStart
    if not timerStart:
        timerStart = True
        cron.start()
    addCount()'''
    details = request.get_json()
    response = write_rpc.call(details)
    return response.decode("utf-8")

@app.route("/api/v1/db/read", methods=["POST"])
def read_deb():
    global cron
    global timerStart
    if not timerStart:
        timerStart = True
        cron.start()
    addCount()
    details = request.get_json()
    response = read_rpc.call(details)
    return response.decode("utf-8")

@app.route("/api/v1/worker/list",methods=["GET"])
def list_worker():
	'''global timerStart
	global cron
	if not timerStart:
		timerStart = True
		cron.start()
	addCount()'''
	client = docker.from_env()
	pid_list = []
	for c in client.containers.list():
		if not (c.name =="rmq" or c.name =="orchestrator" or c.name == "zoo"):
			pid = c.top()['Processes'][0][1]
			pid_list.append(int(pid))
	pid_list.sort()
	return str(pid_list)

@app.route("/api/v1/crash/slave",methods=["POST"])
def slavecrash():
	'''global timerStart
	global cron
	if not timerStart:
		timerStart = True
		cron.start()
	addCount()'''
	client = docker.from_env()
	pid_list = []
	c_list=[]
	for c in client.containers.list():
		if "slave" in c.name:
			pid = c.top()['Processes'][0][1]
			pid_list.append(pid)
			c_list.append(c)
	pid_list.sort()
	pid_to_kill = pid_list[-1]
	for c in c_list:
		if(c.top()['Processes'][0][1] == pid_to_kill):
			c.kill()
			print("KILLED SLAVE (Don't tell anyone or you'll be next ;)")
			break
	return str(pid_to_kill)

@app.route("/api/v1/crash/master", methods=["POST"])
def mastercrash():
	client = docker.from_env()
	pid_list = []
	for c in client.containers.list():
		if "slave" in c.name:
			pid = c.top()["Processes"][0][1]
			pid_list.append((pid, c))
	pid_list.sort(key=lambda x:x[0])
	pid_list[0][1].kill()
	print("MASTER ASSASINATED!\n"*10)
	createSlave()
	return "FINISH... >:)"

@app.route("/api/v1/_count", methods=["GET"])
def getCount():
    return THE_COUNT


@app.route("/api/v1/db/clear",methods = ["POST"])
def clearDb():
	requests.post("http://0.0.0.0:80/api/v1/db/write",json={"table":"UserDetails","insert":"","columns":"","action":"delete", "where":""})
	requests.post("http://0.0.0.0:80/api/v1/db/write",json={"table":"RideDetails","insert":"","columns":"","action":"delete", "where":""})
	return {}



@app.route("/my/test/workers", methods=["POST"])
def listworkers():
	return str(zk.get_children('/workers'))
if __name__ == '__main__':
	app.debug=True
	initCount()
	global global_count
	global_count = 1
	#for i in range(2):
	#	createSlave()
	app.run(host="0.0.0.0", port = 80, use_reloader=False)

