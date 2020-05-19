import time
import numpy as np
from PIL import Image
import os
import random

import logging

import zmq
import sys
import pyarrow as pa

if __name__ == '__main__':
	logging.basicConfig(filename='log_client.txt', level=logging.INFO,
		format='%(asctime)s %(message)s')

	ip = sys.argv[1]
	logging.info('client ip is %s' % ip)

	f = open("/server-config.txt", "r")
	lines = f.readlines()

	server_list = []
	client_list = []

	for i, l in enumerate(lines):
		if i == 0:
			server_list = l.strip().split()
		else:
			client_list = l.strip().split()

	my_position = client_list.index(ip)
	server_ip = server_list[my_position]
	logging.info('corresponding server ip is %s' % server_ip)

	context = zmq.Context()

	bind_socket = context.socket(zmq.PULL)
	bind_port = "5555"
	bind_socket.bind("tcp://*:%s" % bind_port)

	connect_socket = context.socket(zmq.REQ)
	connect_port = "5556"
	connect_socket.connect("tcp://%s:%s" % (server_ip, connect_port))

	logging.info('loading images')
	prefix = 'imagenet'
	files = os.listdir(prefix)
	files = [os.path.join(prefix, fname) for fname in files]
	image_list = []
	for fname in files:
		image_list.append(np.array(Image.open(fname).convert('RGB').resize((224, 224))))

	print('listening for trigger')
	logging.info('listening for trigger')

	while True:
		params = bind_socket.recv_string().split(':')

		logging.info('received trigger')

		respond_ip = params[0]
		num_requests = int(params[1])

		sckt = context.socket(zmq.PUSH)
		sckt.connect('tcp://' + respond_ip + ':3000')


		latencies = []

		for request in range(num_requests):
			if request % 10 == 0:
				print('request number %d' % request)
				logging.info('request number %d' % request)
			start = time.time()
			img = random.choice(image_list)
			payload = pa.serialize(['t', img]).to_buffer().to_pybytes()
			connect_socket.send(payload)
			message = connect_socket.recv()
			end = time.time()
			#print(pa.deserialize(message))
			latencies.append((end - start))
			#print('invocation took %s seconds' % (end - start))
			#logging.info('invocation took %s seconds' % (end - start))

		if len(latencies) > 200:
			del latencies[:200]

		sckt.send(pa.serialize(latencies).to_buffer().to_pybytes())