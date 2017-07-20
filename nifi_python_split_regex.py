

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
            data = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
            
            new_flowfiles = []
            
            splits = re.split(r'\n(?=[0-9]{4}\-[0-9]{2}\-[0-9]{2}[ ]+[0-9]{2}\:[0-9]{2}\:[0-9]{2}\,[0-9]{3})', data)
            
            for split in splits:
                
                splitFlowFile = session.create(self.parentFlowFile)
                writeCallback = WriteCallback()
                writeCallback.content = split
                splitFlowFile = session.write(splitFlowFile, writeCallback)
                '''
                splitFlowFile = session.putAllAttributes(splitFlowFile, {
                    'syslog_year': syslog_year,
                    'syslog_month': syslog_month,
                    'syslog_day': syslog_day,
                    'syslog_hour': syslog_hour,
                    'syslog_minute': syslog_minute,
                    'syslog_second': syslog_second,
                    'syslog_millisecond': syslog_millisecond
                })
                '''
                
                new_flowfiles.append(splitFlowFile)
            
            for splitFlowFile in new_flowfiles:
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
