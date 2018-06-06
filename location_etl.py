"""
location JSON record ETL
"""
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import InputStreamCallback, OutputStreamCallback


class PyInputStreamCallback(InputStreamCallback):

    def __init__(self):
    
        pass
    def process(self, inputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        return text.split('\n')[0]


class PyOutputStreamCallback(OutputStreamCallback):

    def __init__(self):
        pass
        
    def process(self, outputStream):
        outputStream.write(bytearray('Hello World!'.encode('utf-8')))

flowFile = session.get()
if(flowFile != None):
    flowFile = session.write(flowFile, PyOutputStreamCallback())

# end class

flowFile = session.get()
if(flowFile != None):
    session.read(flowFile, PyInputStreamCallback())
    newFlowFile = session.create()

