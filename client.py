import time
import numpy as np
from PIL import Image

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

	for i, l in lines.enumerate():
		if i == 0:
			server_list = l.strip().split()
		else:
			client_list = l.strip().split()

	my_position = client_list.index(ip)
	server_ip = server_list[my_position]
	logging.info('corresponding server ip is %s' % server_ip)

	context = zmq.Context()

	bind_socket = context.socket(zmq.REP)
	bind_port = "5555"
	bind_socket.bind("tcp://*:%s" % bind_port)

	connect_socket = context.socket(zmq.REQ)
	connect_port = "5556"
	connect_socket.connect("tcp://%s:%s" % (server_ip, connect_port))

	print('listening for trigger')
	logging.info('listening for trigger')

	while True:
		num_requests = int(bind_socket.recv_string())

		latencies = []

		fname = 'cat.jpg'
		img = np.array(Image.open(fname).convert('RGB').resize((224, 224)))
		payload = pa.serialize(['t', img]).to_buffer().to_pybytes()

		for request in range(num_requests):
			print('request number %d' % request)
			start = time.time()
			connect_socket.send(payload)
			message = connect_socket.recv()
			end = time.time()
			print(pa.deserialize(message))
			latencies.append((end - start))
			print('invocation took %s seconds' % (end - start))

		bind_socket.send(pa.serialize(latencies).to_buffer().to_pybytes())