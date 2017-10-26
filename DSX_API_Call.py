
import traceback
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback
from org.python.core.util import StringUtil

import subprocess, os
import re,sys
import json
import datetime,time
import random


class WriteCallback(OutputStreamCallback):
    def __init__(self):
        self.content = None
        self.charset = StandardCharsets.UTF_8
    
    def process(self, outputStream):
        bytes = bytearray(self.content.encode('utf-8'))
        outputStream.write(bytes)


class SplitCallback(InputStreamCallback):
  def __init__(self):
    self.parentFlowFile = None
  
  def process(self, inputStream):
    
    splitFlowFile = session.create(self.parentFlowFile)
    writeCallback = WriteCallback()
    
    # To read content as a string:
    data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    vars = data.split(',')
    
    isCertified = vars[0]
    paymentScheme = vars[1]
    hoursDriven = vars[2]
    milesDriven = vars[3]
    latitude = vars[4]
    longitude = vars[5]
    isFoggy = vars[6]
    isRainy = vars[7]
    isWindy = vars[8]
    
    accessToken = splitFlowFile.getAttribute('accessToken')
    
    curl_input = ['curl', '-i', '-k', '-X', 'POST', 'https://172.26.228.121/v2/scoring/online/4a5692ff-2bfc-42d4-a005-ebbd8ffea88a', '-d', '{"fields":["isCertified","paymentScheme","hoursDriven","milesDriven","latitude","longitude","isFoggy","isRainy","isWindy"],"records":[["' + str(isCertified) + '","' + str(paymentScheme) + '",' + str(hoursDriven) + ',' + str(milesDriven) + ',' + str(latitude) + ',' + str(longitude) + ',' + str(isFoggy) + ',' + str(isRainy) + ',' + str(isWindy) + ']]}', '-H', 'content-type:application/json', '-H', str('authorization: Bearer ' + str(accessToken))]
    
    result = subprocess.Popen(curl_input, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    out, err = result.communicate()
    
    predictions = json.loads(out.split('\r\n')[-1])['records'][0][-3:]
    
    prediction_prob_0 = str(predictions[0][0])
    prediction_prob_1 = str(predictions[0][1])
    prediction        = str(predictions[1])
    prediction_label  = str(predictions[2])
    
    #predictions_csv = str(predictions[0][0]) + ',' + str(predictions[0][1]) + ',' + str(predictions[1]) + ',' + str(predictions[2])
    payload = 'payload'
    writeCallback.content = payload
    splitFlowFile = session.write(splitFlowFile, writeCallback)
    splitFlowFile = session.putAllAttributes(splitFlowFile, {
            'prediction_prob_0': prediction_prob_0,
            'prediction_prob_1': prediction_prob_1,
            'prediction': prediction,
            'prediction_label':prediction_label
        })
    
    session.transfer(splitFlowFile, REL_SUCCESS)
    
    # Write modified content
    #outstream.write(str(output))


parentFlowFile = session.get()
if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)


#ZEND
