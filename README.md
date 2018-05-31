# ExecuteScript Handbook For Python

[Original post links](https://community.hortonworks.com/articles/75032/executescript-cookbook-part-1.html)


## Part 1. NiFi API and FlowFiles:

- Get an incoming flow file from the session

```python
flowFile = session.get() 
if (flowFile != None):
    # All processing code starts at this indent
# implicit return at the end
```

- Get multiple incoming flow files from the session

```python
flowFileList = session.get(100)
if not flowFileList.isEmpty():
    for flowFile in flowFileList: 
         # Process each FlowFile here
```

- Create a new FlowFile

```python
flowFile = session.create() 
# Additional processing here
```

- Create a new FlowFile from a parent FlowFile

```python
flowFile = session.get() 
if (flowFile != None):
    newFlowFile = session.create(flowFile) 
    # Additional processing here
```

- Add an attribute to a flow file

```python
flowFile = session.get() 
if (flowFile != None):
    flowFile = session.putAttribute(flowFile, 'myAttr', 'myValue')
# implicit return at the end
```

- Add multiple attributes to a flow file

```python
attrMap = {'myAttr1':'1', 'myAttr2':str(2)}
flowFile = session.get() 
if (flowFile != None):
    flowFile = session.putAllAttributes(flowFile, attrMap)
# implicit return at the end
```

- Get an attribute from a flow file

```python
flowFile = session.get() 
if (flowFile != None):
    myAttr = flowFile.getAttribute('filename')
# implicit return at the end
```

- Get all attributes from a flow file

```python
flowFile = session.get() 
if (flowFile != None):
    for key,value in flowFile.getAttributes().iteritems():
       # Do something with key and/or value
# implicit return at the end
```

- Transfer a flow file to a relationship

```python
flowFile = session.get() 
if (flowFile != None):
    # All processing code starts at this indent
    if errorOccurred:
        session.transfer(flowFile, REL_FAILURE)
    else:
        session.transfer(flowFile, REL_SUCCESS)
# implicit return at the end
```

- Send a message to the log at a specified logging level

```python
from java.lang import Object
from jarray import array
objArray = ['Hello',1,True]
javaArray = array(objArray, Object)
log.info('Found these things: {} {} {}', javaArray)
```

## Part 2: FlowFile I/O and Error Handling

- Read the contents of an incoming flow file using a callback

```python
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import InputStreamCallback
 
# Define a subclass of InputStreamCallback for use in session.read()
class PyInputStreamCallback(InputStreamCallback):

    def __init__(self):
    
        pass
    def process(self, inputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    # Do something with text here
# end class

flowFile = session.get()
if(flowFile != None):
    session.read(flowFile, PyInputStreamCallback())
# implicit return at the end
```

- Write content to an outgoing flow file using a callback

```python
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import OutputStreamCallback
 
# Define a subclass of OutputStreamCallback for use in session.write()
class PyOutputStreamCallback(OutputStreamCallback):

    def __init__(self):
        pass
        
    def process(self, outputStream):
        outputStream.write(bytearray('Hello World!'.encode('utf-8')))
# end class

flowFile = session.get()
if(flowFile != None):
    flowFile = session.write(flowFile, PyOutputStreamCallback())
# implicit return at the end
```

- Overwrite an incoming flow file with updated content using a callback

```python
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback
 
# Define a subclass of StreamCallback for use in session.write()
class PyStreamCallback(StreamCallback):

  def __init__(self):
        pass
        
  def process(self, inputStream, outputStream):
    text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(bytearray('Hello World!'[::-1].encode('utf-8')))
# end class

flowFile = session.get()
if(flowFile != None):
    flowFile = session.write(flowFile, PyStreamCallback())
# implicit return at the en
```

- Handle errors during script processing

```python
flowFile = session.get()
if(flowFile != None):
    try:
        # Something that might throw an exception here
        # Last operation is transfer to success (failures handled in the catch block)
        session.transfer(flowFile, REL_SUCCESS)
    except:
        log.error('Something went wrong', e)
        session.transfer(flowFile, REL_FAILURE)
# implicit return at the end
```

## Part 3: Advanced Features

- Dynamic Properties

  * Get the value of a dynamic property
  
    Add <*Property*>:<*Value*> pair to **Configure Processor**, and then access them via `<Property>.getValue()`

    ```python
    myValue1 = myProperty1.getValue()
    ```
    
  * Get the value of a dynamic property after evaluating Expression Language constructs
    
    ```python
    myValue1 = myProperty1.getValue()
    myValue2 = myProperty2.evaluateAttributeExpressions(flowFile).getValue()
    ```

- Adding Modules

  * Add Python site-packages folder to the **Module Directory** property, such as `/usr/local/lib/python3/site-packages`
  

- Accessing Controller Services