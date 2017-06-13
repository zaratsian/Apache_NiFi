
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
                
                timestamp_full = re.findall('[0-9\-]{10} [0-9\:]{8}\,[0-9]{3}',record)[0]
                timestamp_key  = re.findall('[0-9\-]{10} [0-9\:]{5}',record)[0]
                
                if timestamp_key in groups:
                    groups[timestamp_key].append([record, timestamp_full])
                else:
                    groups[timestamp_key] = [[record, timestamp_full]]
            
            for k,v in groups.iteritems():
                
                timestamp_full      = v[0][1]
                timestamp_obj       = datetime.datetime.strptime(re.findall('[0-9\-]{10} [0-9\:]{8}\,[0-9]{3}', timestamp_full )[0], '%Y-%m-%d %H:%M:%S,%f')
                
                syslog_year         = str(timestamp_obj.year)
                syslog_month        = str(timestamp_obj.month).zfill(2)
                syslog_day          = str(timestamp_obj.day).zfill(2)
                syslog_hour         = str(timestamp_obj.hour).zfill(2)
                syslog_minute       = str(timestamp_obj.minute).zfill(2)
                syslog_second       = str(timestamp_obj.second).zfill(2)
                syslog_millisecond  = str(timestamp_obj.microsecond)[:3]
                
                payload = '\n'.join([item[0] for item in v])
                
                splitFlowFile = session.create(self.parentFlowFile)
                writeCallback = WriteCallback()
                writeCallback.content = payload
                splitFlowFile = session.write(splitFlowFile, writeCallback)
                splitFlowFile = session.putAllAttributes(splitFlowFile, {
                    'syslog_year': syslog_year,
                    'syslog_month': syslog_month,
                    'syslog_day': syslog_day,
                    'syslog_hour': syslog_hour,
                    'syslog_minute': syslog_minute,
                    'syslog_second': syslog_second,
                    'syslog_millisecond': syslog_millisecond
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
