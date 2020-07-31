import pika
import json
import sqlalchemy as sql
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from random import randint
import ast
from kazoo.client import KazooClient
import docker
import os
import sys


def getAllWorkersPID():
	client = docker.from_env()
	pid_list = []
	c = client.containers.list()
	#print("ALL CONTAINERS:", c)
	for c in client.containers.list():
		#print("CONTAINER:", c.name)
		if 'slave' in c.name:
			pid = c.top()['Processes'][0][1]
			pid_list.append(int(pid))
	pid_list.sort()
	return pid_list

def getMyPID():
	cont_id = os.popen("hostname").read().strip()
	client = docker.from_env()
	cont = client.containers.get(cont_id)
	myPID = cont.top()['Processes'][0][1]
	return int(myPID)


def getMyName():
	cont_id = os.popen("hostname").read().strip()
	client = docker.from_env()
	cont = client.containers.get(cont_id)
	return cont.name

def iAmTheMaster():
	allPID = getAllWorkersPID()
	#print("GOT IT BOSS!")
	myPID = getMyPID()
	#print(allPID, myPID)
	if myPID == allPID[0]:
		print("I'm the master y'all!\n"*10)
		return True
	else:
		print(type(myPID), type(allPID[0]))


#zk.KazooClient(hosts='127.0.0.1:2181')
zk = KazooClient(hosts='zoo:2182')
zk.start()

if zk.exists("/workers"):
    zk.create("/workers/worker"+str(getMyPID())+":::"+getMyName(), b'WORKER', ephemeral=True)
else:
    print("WORKER HEAD IS MISSING...")


if zk.exists("/election"):
    zk.create("/election/candidate"+str(getMyPID()), b'CANDIDATE', ephemeral=True)
else:
    print("ELECTION NOT DECLARED...")



@zk.ChildrenWatch("/election")
def watch_children(children):
    print("MASTER MUST BE GONE...\n"*10)
    if iAmTheMaster():
        os.execl(sys.executable, 'python3', 'master.py')
        exit(0)


engine = sql.create_engine('sqlite:///database/RideShare.db', echo=True)
meta = sql.MetaData()
user_details = Table('UserDetails', meta, 
	Column('username', String, primary_key=True),
	Column('password', String),
	)
ride_details = Table('RideDetails', meta, 
	Column('ride_id', Integer, primary_key=True),
	Column('created_by', String),
	Column('source', String),
	Column('destination', String),
	Column('timestamp', String),
	Column('riders_list', String),
	)


meta.create_all(engine)


def write_db(queryData):
	try:
		values = queryData['insert'] 
		columns = queryData['columns'] 
		table = queryData['table']
		action = queryData['action']
		condition = queryData['where']

		if action == "insert":
			conn = engine.connect()
			query = "INSERT INTO " + table + "("
			for i in columns:
				query += i + ","
			query = query[:-1] + ") VALUES("
			for i in values:
				if type(i) == str:
					query += "'" + i + "',"
				elif type(i) == int:
					query += str(i) + ","
				else:
					print("UNSUPPORTED DATA-TYPE")
					exit(0)
			query = query[:-1] + ")"
			# print(".\n"*5 + query)
			conn.execute(query)
			response = "DONE"

		elif action == "update":
			conn = engine.connect()
			query = "UPDATE " + table + " SET " + columns[0] + "='" + values + "' WHERE " + condition
			# print(".\n"*5 + query)
			conn.execute(query)
			response = "DONE"
			

		elif action == "delete":
			conn = engine.connect()
			query = "DELETE FROM "+ table + " WHERE " + condition
			# print(query)
			conn.execute(query)
			response = "DONE"
		
		else:
			response = "UNKNOWN action"
	except KeyError:
		response = "Please provide proper JSON request body"
	return response


def read_db(queryData):
	try:
		table = queryData['table']
		columns = queryData['columns']
		condition = queryData['where']
		# print(".\n"*10)
		conn = engine.connect()
		# res = conn.execute("SELECT * FROM RideDetails")
		# res = conn.execute("SELECT ride_id,created_by FROM RideDetails")
		query = "SELECT " + ",".join(columns) + " FROM " + table
		if condition:
			query += " WHERE " + condition
		# print(".\n" * 5, query)
		res = conn.execute(query)
		res = list(res)
		for index, _ in enumerate(res):
			res[index] = tuple(res[index])
		# print(res)
		response = json.dumps(res)
	except Exception as e:
		print("Error is:", e)
		response = "Please provide proper JSON request body"
	return response





def on_request(ch, method, props, body):
    body = body.decode("utf-8")
    print(body, "\n"*10)
    #queryData = json.loads(body)
    queryData = ast.literal_eval(body)
    print(queryData, type(queryData), "\n"*10)
    response = read_db(queryData)
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)



connection = pika.BlockingConnection(pika.ConnectionParameters(host='rmq'))
channel = connection.channel()
channel.queue_declare(queue='readQ')
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='readQ', on_message_callback=on_request)


channel2 = connection.channel()
channel2.exchange_declare(exchange='syncq', exchange_type='fanout')
result = channel2.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel2.queue_bind(exchange='syncq', queue=queue_name)
def callback(ch, method, properties, body):
	print("received ",body.decode("utf-8"))
	query = body.decode("utf-8")
	conn = engine.connect()
	conn.execute(query)

	
channel2.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

channel2.start_consuming()
channel.start_consuming()
