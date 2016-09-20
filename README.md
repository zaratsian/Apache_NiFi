<h3>NiFi Template - Execute Python Script</h3>
<br>
<br>This <a href="https://nifi.apache.org/">Apache NiFi</a> tempalate show how to execute a python script with the NiFi flow. More importantly, it shows how to capture the input and output flowFile metadata. This has been tested within NiFi 1.0.0.
<br>
<br>Here's what the processor does:
<br>1.) Read data.json file
<br>2.) Use ExecuteScript processor to parse json and perform arbitrary python processing.
<br>3.) Write attributes to json
<br>4.) From here, the results can be saved to a simple .json file, to HDFS, etc.
<br>
<br>Reference: <a href="https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html">NiFi Expression Language</a>
