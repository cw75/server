import sys
import time
import numpy as np
from PIL import Image

import zmq

import pyarrow as pa

def print_latency_stats(data, ident):
    npdata = np.array(data)

    mean = np.mean(npdata)
    median = np.percentile(npdata, 50)
    p75 = np.percentile(npdata, 75)
    p95 = np.percentile(npdata, 95)
    p99 = np.percentile(npdata, 99)
    mx = np.max(npdata)

    p25 = np.percentile(npdata, 25)
    p05 = np.percentile(npdata, 5)
    p01 = np.percentile(npdata, 1)
    mn = np.min(npdata)

    output = ('%s LATENCY:\n\tsample size: %d\n' +
              '\tmean: %.6f, median: %.6f\n' +
              '\tmin/max: (%.6f, %.6f)\n' +
              '\tp25/p75: (%.6f, %.6f)\n' +
              '\tp5/p95: (%.6f, %.6f)\n' +
              '\tp1/p99: (%.6f, %.6f)') % (ident, len(data), mean, median, mn,
                                           mx, p25, p75, p05, p95, p01, p99)
    print(output)

if __name__ == '__main__':

  ips = []
  with open('client_ips.txt', 'r') as f:
    line = f.readline()
    while line:
      ips.append(line.strip())
      line = f.readline()

  msg = sys.argv[1]

  context = zmq.Context()
  recv_socket = context.socket(zmq.PULL)
  recv_socket.bind('tcp://*:3000')

  sent_msgs = 0

  for ip in ips:
    sckt = context.socket(zmq.PUSH)
    sckt.connect('tcp://' + ip + ':5555')
    sckt.send_string(msg)
    sent_msgs += 1

  total = []
  total_throughput = 0.0
  end_recv = 0

  while end_recv < sent_msgs:
    latencies, throughput = pa.deserialize(recv_socket.recv())
    total_throughput += throughput
    total += latencies
    end_recv += 1

  print_latency_stats(total, 'nmt')
  print('throughput is: %f ops/s' % total_throughput)