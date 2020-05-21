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

def invoke(endpoint, payload, custom_attributes):
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

	while True:
		payload = socket.recv()

		#print('Transform Stage')
		#logging.info('Transform Stage')
		#transformed_img = pa.deserialize(invoke('cascade-transform', payload))
		serialized_transformed_img = invoke('cascade-transform', payload, 't')
		#print('Resnet Stage')
		#logging.info('Resnet Stage')
		#res_index, res_prob = pa.deserialize(invoke('cascade-resnet', payload))
		res_index, res_prob = pa.deserialize(invoke('cascade-resnet-gpu', serialized_transformed_img, 'r'))
		#print('Inception Stage')
		#logging.info('Inception Stage')
		transformed_img = pa.deserialize(serialized_transformed_img)
		payload = pa.serialize([transformed_img, res_prob]).to_buffer().to_pybytes()
		#ic_index, ic_prob = pa.deserialize(invoke('cascade-inception', payload))
		ic_index, ic_prob = pa.deserialize(invoke('cascade-inception-gpu', payload, 'i'))
		#print('Cascade Stage')
		#logging.info('Cascade Stage')
		payload = pa.serialize([res_index, res_prob, ic_index, ic_prob]).to_buffer().to_pybytes()
		#result = pa.deserialize(invoke('cascade', payload))
		#socket.send(invoke('cascade-cascade', payload))
		socket.send(invoke('cascade-cascade', payload, 'c'))
		#print(result)

	#end = time.time()
	#print('invocation took %s seconds' % (end - start))
