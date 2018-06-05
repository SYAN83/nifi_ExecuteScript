"""
FetchMongo ExecuteScript
"""

# Authors: Shu Yan <yanshu.usc@gmail.com>
# NiFi Ver: 1.6.0

# Required Property:
#     None

# import nifi libraries
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

# import python libraries
import json


class PyStreamCallback(StreamCallback):

    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        obj = json.loads(IOUtils.toString(inputStream, StandardCharsets.UTF_8))
        if isinstance(obj, dict):
            text = obj.get('_id', 'none')
        elif isinstance(obj, list):
            text = str([x.get('_id', 'none') for x in obj])
        else:
            text = 'none'
        outputStream.write(bytearray(text.encode('utf-8')))

# Get flowFile (trigger)
flowFile = session.get()

if flowFile:
    flowFile = session.write(flowFile, PyStreamCallback())
    try:
        flowFile = session.write(flowFile, PyStreamCallback())
        session.transfer(flowFile, REL_SUCCESS)
    except Exception as e:
        log.error(e)
        session.transfer(flowFile, REL_FAILURE)