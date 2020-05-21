import time
import numpy as np
from PIL import Image
import os
import random

import logging

import zmq
import sys
import pyarrow as pa

from torchvision import transforms
import cv2

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

	logging.info('loading video')

	transform = transforms.Compose([
		transforms.ToPILImage(),
		transforms.CenterCrop((720, 720)),
		transforms.Resize((256, 256)),
		transforms.ToTensor()
	])
	images = []
	cap = cv2.VideoCapture('videos/steph3.mp4')
	more_frames, frame = cap.read()
	while more_frames:
		frame = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)
		frame = transform(frame).detach().numpy()
		images.append(frame)
		more_frames, frame = cap.read()

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
			segment = 0

			while segment < 5:
				payload = pa.serialize(images[6*segment:6*segment+6]).to_buffer().to_pybytes()
				connect_socket.send(payload)
				message = connect_socket.recv()
				#logging.info(pa.deserialize(message))
				segment += 1

			end = time.time()

			latencies.append((end - start))

		if len(latencies) > 200:
			del latencies[:200]

		throughput = len(latencies) / sum(latencies)

		sckt.send(pa.serialize([latencies, throughput]).to_buffer().to_pybytes())