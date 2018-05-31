import json
import java.io
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback


class PyStreamCallback(StreamCallback):

    def __init__(self, callback=None, **kwargs):
        if callable(callback):
            self.callback = callback
        self.kwargs = kwargs

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        if self.callback:
            text = self.callback(obj, **kwargs)
        outputStream.write(bytearray(text))


def get_id(text, **kwargs):
    try:
        obj = json.loads(text)
    except ValueError:
        obj = dict()
    obj.update(kwargs)
    return json.dumps(obj, indent=2).encode('utf-8')


flowFile = session.get()
if flowFile is None:
    session.transfer(flowFile, REL_FAILURE)
else:
    flowFile = session.write(flowFile, 
                             PyStreamCallback(callback=get_id, 
                                              collection=flowFile.getAttribute('http.query.param.collection')))
    flowFile = session.putAttribute(flowFile, "filename",
                                    flowFile.getAttribute('filename') + __name__ + '.json')