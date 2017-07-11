
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

import re,sys
import json
import datetime,time
import random

class PyStreamCallback(StreamCallback):
  def __init__(self): pass
  def process(self, instream, outstream):
    
    # To read content as a string:
    data = IOUtils.toString(instream, StandardCharsets.UTF_8)
    
    output = {}
    
    datetimestamp = datetime.datetime.now()
    
    output['id']        = datetimestamp.strftime('%Y%m%d_%H%M%S')
    output['datetime']  = datetimestamp.strftime('%Y-%m-%d %H:%M:%S')
    output['state']     = random.choice(['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'])
    output['duration']  = round(random.triangular(1,150,1),2)
    output['action']    = random.choice(['TRUE']*1 + ['FALSE']*5)   # True/False ratio of 1:5   
    
    # Generate CSV output based on "output", which is in JSON
    output_csv = ','.join([str(v) for k,v in output.items()])
    
    # Write modified content
    outstream.write(str(output))


flowfile = session.get()


if (flowfile != None):
    
    time.sleep(1)
    
    # Read FlowFile Attribute
    # myVar = flowfile.getAttribute('filename')
    
    # Write variable/values to FlowFile Attributes
    #flowFile = session.putAttribute(flowFile, "from_python_string", "python string example")
    #flowFile = session.putAttribute(flowFile, "from_python_number", str(new_value))
    
    # Write data to FlowFile
    flowfile = session.write(flowfile, PyStreamCallback())
    session.transfer(flowfile, REL_SUCCESS)


#ZEND
