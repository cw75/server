from multiprocessing import Pool
import boto3
import time
import numpy as np
from PIL import Image

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
	#fname = 'cat.jpg'
	#img = np.array(Image.open(fname).convert('RGB').resize((224, 224)))
	#payload = pa.serialize(['t', img]).to_buffer().to_pybytes()
	#start = time.time()

	context = zmq.Context()
	socket = context.socket(zmq.REP)
	port = "5556"
	socket.bind("tcp://*:%s" % port)

	print('listening for request')

	while True:
		payload = socket.recv()

		print('Transform Stage')
		transformed_img = pa.deserialize(invoke('cascade', payload))
		print('Resnet Stage')
		payload = pa.serialize(['r', transformed_img]).to_buffer().to_pybytes()
		res_index, res_prob = pa.deserialize(invoke('cascade', payload))
		print('Inception Stage')
		payload = pa.serialize(['i', transformed_img, res_prob]).to_buffer().to_pybytes()
		ic_index, ic_prob = pa.deserialize(invoke('cascade', payload))
		print('Cascade Stage')
		payload = pa.serialize(['c', res_index, res_prob, ic_index, ic_prob]).to_buffer().to_pybytes()
		#result = pa.deserialize(invoke('cascade', payload))
		socket.send(invoke('cascade', payload))
		#print(result)

	#end = time.time()
	#print('invocation took %s seconds' % (end - start))
