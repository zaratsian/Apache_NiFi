
#######################################################################################
#
#   Input Flowfile Format:
#       2017-01-25 12:00:00 AAAA BBBB CCCC
#       2017-01-25 12:00:30 AAAA BBBB CCCC
#       2017-01-25 12:01:00 AAAA BBBB CCCC
#       2017-01-25 12:01:30 AAAA BBBB CCCC
#       2017-01-25 12:02:00 AAAA BBBB CCCC
#       2017-01-25 12:02:30 AAAA BBBB CCCC
#
#   Output Flowfile Format (parsed at the minute level):
#       Flowfile 1 will contain:
#       2017-01-25 12:00:00 AAAA BBBB CCCC
#       2017-01-25 12:00:30 AAAA BBBB CCCC
#        
#       Flowfile 2 will contain:
#       2017-01-25 12:01:00 AAAA BBBB CCCC
#       2017-01-25 12:01:30 AAAA BBBB CCCC
#
#   NOTE: This code should be placed within the NiFi "ExecuteScript" Processor
#
#######################################################################################

import json
import sys, re
import datetime, time
import traceback
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback
from org.python.core.util import StringUtil


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
        try:
            # Read input FlowFile content
            input_text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
            input_list = input_text.split('\n')
            
            # Create FlowFiles for array items
            splits = []
            
            groups = {}
            
            for record in input_list:
                timestamp_key = re.findall('[0-9\-]{10} [0-9\:]{5}',record)[0]
                if timestamp_key in groups:
                    groups[timestamp_key].append(record)
                else:
                    groups[timestamp_key] = [record]
            
            for k,v in groups.iteritems():
                
                payload = '\n'.join(v)
                
                splitFlowFile = session.create(self.parentFlowFile)
                writeCallback = WriteCallback()
                writeCallback.content = payload
                splitFlowFile = session.write(splitFlowFile, writeCallback)
                splitFlowFile = session.putAllAttributes(splitFlowFile, {
                    'color': 'blue'
                })
                
                splits.append(splitFlowFile)
            
            for splitFlowFile in splits:
                session.transfer(splitFlowFile, REL_SUCCESS)
        except:
            traceback.print_exc(file=sys.stdout)
            raise


parentFlowFile = session.get()
if parentFlowFile != None:
    splitCallback = SplitCallback()
    splitCallback.parentFlowFile = parentFlowFile
    session.read(parentFlowFile, splitCallback)
    session.remove(parentFlowFile)


#ZEND
