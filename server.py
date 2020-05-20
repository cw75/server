from multiprocessing import Pool
import boto3
import time
import numpy as np
from PIL import Image

import logging

import zmq

import pyarrow as pa

client = boto3.client('sagemaker-runtime')

#endpoint_name = 'composition'
content_type = 'text/plain'

def invoke(endpoint, payload):
	response = client.invoke_endpoint(
	    EndpointName=endpoint, 
	    ContentType=content_type,
	    Body=payload
	    )
	return response['Body'].read()


if __name__ == '__main__':
	logging.basicConfig(filename='log_server.txt', level=logging.INFO,
		format='%(asctime)s %(message)s')

	context = zmq.Context()
	socket = context.socket(zmq.REP)
	port = "5556"
	socket.bind("tcp://*:%s" % port)

	print('listening for request')
	logging.info('listening for request')

	while True:
		inp, english_sentence = pa.deserialize(socket.recv())

		payload = pa.serialize(inp).to_buffer().to_pybytes()

		#logging.info('Classification Stage')
		language = pa.deserialize(invoke('nmt-c', payload))

		#logging.info('Translation Stage')
		payload = pa.serialize(english_sentence).to_buffer().to_pybytes()
		if language == 'fr':
			socket.send(invoke('nmt-f-gpu', payload))
		elif language == 'de':
			socket.send(invoke('nmt-g-gpu', payload))
		else:
			logging.error('Unexpected Language Type')