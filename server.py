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
		payload = socket.recv()

		cat = pa.deserialize(invoke('rec-dummy', payload))
		uid, _ = pa.deserialize(payload)

		payload = pa.serialize([uid, cat]).to_buffer().to_pybytes()
		result = invoke('rec-vpc-redis', payload)

		socket.send(result)