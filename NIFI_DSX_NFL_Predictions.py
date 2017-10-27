
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
    try:        
        # To read content as a string:
        data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        vars = data.split(',')
        
        down = vars[0]
        qtr = vars[1]
        month_day = vars[2]
        playtype_lag_index = vars[3]
        timesecs = vars[4]
        ydsnet = vars[5]
        ydstogo = vars[6]
        yrdline100 = vars[7]
        
        accessToken = splitFlowFile.getAttribute('accessToken')
        
        curl_input = ['curl', '-i', '-k', '-X', 'POST', 'https://172.26.228.121/v2/scoring/online/32b1d108-369d-42ce-966b-48d0a20a6b38', '-d', '{"fields":["down","qtr","month_day","PlayType_lag_index","TimeSecs","ydsnet","ydstogo","yrdline100"],"records":[[' + str(down) + ',' + str(qtr) + ',' + str(month_day) + ',"' + str(playtype_lag_index) + '",' + str(timesecs) + ',' + str(ydsnet) + ',' + str(ydstogo) + ',' + str(yrdline100) + ']]}', '-H', 'content-type:application/json', '-H', str('authorization: Bearer ' + str(accessToken))]
        
        result = subprocess.Popen(curl_input, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        out, err = result.communicate()
        
        prediction_results       = json.loads(out.split('\r\n')[-1])['records'][0]
        predictions_yards_gained = str(prediction_results[-1])
        
        #predictions_csv = str(predictions[0][0]) + ',' + str(predictions[0][1]) + ',' + str(predictions[1]) + ',' + str(predictions[2])
        payload = ','.join([str(record) for i,record in enumerate(prediction_results) if i != 8])
        writeCallback.content = payload
        splitFlowFile = session.write(splitFlowFile, writeCallback)
        splitFlowFile = session.putAllAttributes(splitFlowFile, {
                'predictions_yards_gained': predictions_yards_gained
            })
    
    except:
        pass
    
    session.transfer(splitFlowFile, REL_SUCCESS)


parentFlowFile = session.get()
if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)


#ZEND
