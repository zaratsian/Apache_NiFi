<h3>NiFi Template - Execute Python Script</h3>
<br>This <a href="https://nifi.apache.org/">Apache NiFi</a> tempalate show how to execute a python script with the NiFi flow. More importantly, it shows how to capture the input and output flowFile metadata. This has been tested within NiFi 1.0.0.
<br>
<br>Here's the NiFi flow:
<br>1.) Read data.json file
<br>2.) Use the ExecuteScript processor to parse json and perform arbitrary python processing.
<br>3.) Write attributes to json
<br>4.) From here, the results can be saved to a simple .json file, to HDFS, sent to Kafka, Solr, etc.
<br>
<br><b>Reference:</b> 
<br><a href="https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html">NiFi Expression Language</a>
<br><a href="https://community.hortonworks.com/articles/75032/executescript-cookbook-part-1.html">ExecuteScript Cookbook - Part 1</a>
<br><a href="https://community.hortonworks.com/articles/75545/executescript-cookbook-part-2.html">ExecuteScript Cookbook - Part 2</a>
<br><a href="https://community.hortonworks.com/content/kbentry/77739/executescript-cookbook-part-3.html">ExecuteScript Cookbook - Part 3</a>
