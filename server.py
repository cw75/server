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

def invoke_parallel(inp):
	endpoint, payload, custom_attributes = inp
	response = client.invoke_endpoint(
	    EndpointName=endpoint, 
	    ContentType=content_type,
	    CustomAttributes=custom_attributes,
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

	p = Pool(2)

	while True:
		payload = socket.recv()

		logging.info('Yolo Stage')
		result = invoke('video-yolo-gpu', payload)

		logging.info('Transform Stage')
		result = invoke('video-transform', result)

		logging.info('Parallel Resnet Stage')
		results = p.map(invoke_parallel, [('video-resnet-person-gpu', result, 'p'), ('video-resnet-vehicle-gpu', result, 'v')])

		accumulated = []
		for payload in results:
			accumulated += pa.deserialize(payload)

		socket.send(pa.serialize(accumulated).to_buffer().to_pybytes())