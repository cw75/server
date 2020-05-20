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

	logging.info('loading sentences')

	french = [
		'Je m\'appelle Pierre.',
		'Comment allez-vous aujourd\'hui?',
		'La nuit est longue et froide, et je veux rentrer chez moi.',
		'Tu es venue a minuit, mais je me suis déja couché.',
		'On veut aller dehors mais il faut rester dedans.'
	]

	german = [
		'Ich bin in Berliner.',
		'Die katz ist saß auf dem Stuhl.',
		'Sie schwimmt im Regen.',
		'Ich gehe in den Supermarkt, aber mir ist kalt.',
		'Ich habe nie gedacht, dass du Amerikanerin bist.'
	]

	english = [
		'What is the weather like today?',
		'Why does it rain so much in April?',
		'I like running but my ankles hurt.',
		'I should go home to eat dinner before it gets too late.',
		'I would like to hang out with my friends, but I have to work.'
	]

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

			if random.random() < 0.5:
				other = random.choice(french)
			else:
				other = random.choice(german)
			vals = [other, random.choice(english)]

			start = time.time()
			payload = pa.serialize(vals).to_buffer().to_pybytes()
			connect_socket.send(payload)
			message = connect_socket.recv()
			end = time.time()
			logging.info('translation is: %s' % pa.deserialize(message))
			latencies.append((end - start))

		if len(latencies) > 200:
			del latencies[:200]

		sckt.send(pa.serialize(latencies).to_buffer().to_pybytes())