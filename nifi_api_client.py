
############################################################################################
#
#   NiFi APIs (https://nifi.apache.org/docs/nifi-docs/rest-api/)
#
#   This script provides many functions that allow users to monitor NiFi, such as:
#       - General System Diagnostics (heap utilization, content and flowfile storage, etc.)
#       - NiFi status (number of running and stopped processors, flowfiles in queue, etc.)
#       - Status for each individual processor
#       - Status for each individual connection
#
#   Usage:
#       nifi_api_client.py 'http://dzaratsian-hdf0.field.hortonworks.com:9090/'
#
############################################################################################

import requests
import json



try:
    nifi_host = sys.argv[1]
except:
    print '[ ERROR ] Invalid syntax. NiFi hostname my be incorrect.'
    print '\nUSAGE:' 
    print '\tnifi_api_client.py <nifi_host>'
    print "\tnifi_api_client.py 'http://dzaratsian-hdf0.field.hortonworks.com:9090/'"
    sys.exit()



def nifi_systemDiagnostics(nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/system-diagnostics'
    req  = requests.get(url)
    data = json.loads(req.content)
    heap_pct_used       = data['systemDiagnostics']['aggregateSnapshot']['heapUtilization']
    heap_size           = data['systemDiagnostics']['aggregateSnapshot']['totalHeap']
    content_repo_usage  = data['systemDiagnostics']['aggregateSnapshot']['contentRepositoryStorageUsage'][0]['utilization']
    content_repo_size   = data['systemDiagnostics']['aggregateSnapshot']['contentRepositoryStorageUsage'][0]['totalSpace']
    flowfile_repo_usage = data['systemDiagnostics']['aggregateSnapshot']['flowFileRepositoryStorageUsage']['utilization']
    flowfile_repo_size  = data['systemDiagnostics']['aggregateSnapshot']['flowFileRepositoryStorageUsage']['totalSpace']
    print '[ INFO ] Heap Usage:             ' + str(heap_pct_used)
    print '[ INFO ] Heap Size:              ' + str(heap_size)
    print '[ INFO ] Content Repo Usage:     ' + str(content_repo_usage)
    print '[ INFO ] Content Repo Size:      ' + str(content_repo_size)
    print '[ INFO ] Flowfile Repo Usage:    ' + str(flowfile_repo_usage)
    print '[ INFO ] Flowfile Repo Size:     ' + str(flowfile_repo_size)
    return data


                                            
def nifi_status(nifi_host):
    url    = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/status'   
    req    = requests.get(url)
    data   = json.loads(req.content)
    processors_stopped  = data['controllerStatus']['stoppedCount']
    processors_running  = data['controllerStatus']['runningCount']
    queued_flowfiles    = data['controllerStatus']['queued']
    invalid_processors  = data['controllerStatus']['invalidCount']
    active_thread_count = data['controllerStatus']['activeThreadCount']
    print '[ INFO ] Processors Running:     ' + str(processors_stopped)
    print '[ INFO ] Processors Stopped:     ' + str(processors_running)
    print '[ INFO ] Flowfiles in Queue:     ' + str(queued_flowfiles)
    print '[ INFO ] Invalid Processors:     ' + str(invalid_processors)
    print '[ INFO ] Active Thread Count:    ' + str(active_thread_count)
    return data



def nifi_metadata(nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/search-results'   
    req  = requests.get(url)
    data = json.loads(req.content)
    list_process_group  = [process_group['id'] for process_group in data['searchResultsDTO']['processGroupResults']]
    list_processors     = [processor['id'] for processor in data['searchResultsDTO']['processorResults']]
    list_connections    = [connection['id'] for connection in data['searchResultsDTO']['connectionResults']]
    list_inputports     = [inputport['id'] for inputport in data['searchResultsDTO']['inputPortResults']]
    list_outputports    = [outputport['id'] for outputport in data['searchResultsDTO']['outputPortResults']]
    print '[ INFO ] Total Process Groups:   ' + str(len(list_process_group))
    print '[ INFO ] Total Processors:       ' + str(len(list_processors))
    print '[ INFO ] Total Connections:      ' + str(len(list_connections))
    print '[ INFO ] Total Input Ports:      ' + str(len(list_inputports))
    print '[ INFO ] Total Output Ports:     ' + str(len(list_outputports))
    return list_process_group, list_processors, list_connections



def nifi_processor_status(processor_id, nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/processors/' + str(processor_id) + '/status'   
    req  = requests.get(url)
    data = json.loads(req.content)
    print '[ INFO ] Processor ID:           ' + str(processor_id)
    print '[ INFO ] Processor Name:         ' + str(data['processorStatus']['name'])
    print '[ INFO ] Processor GroupID:      ' + str(data['processorStatus']['groupId'])
    print '[ INFO ] Processor Run Status:   ' + str(data['processorStatus']['aggregateSnapshot']['runStatus'])
    print '[ INFO ] FlowFiles In:           ' + str(data['processorStatus']['aggregateSnapshot']['flowFilesIn'])
    print '[ INFO ] FlowFiles Out:          ' + str(data['processorStatus']['aggregateSnapshot']['flowFilesOut'])
    print '[ INFO ] Active Thread Count:    ' + str(data['processorStatus']['aggregateSnapshot']['activeThreadCount'])
    return data



def nifi_connection_status(connection_id, nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/connections/' + str(connection_id) + '/status'   
    req  = requests.get(url)
    data = json.loads(req.content)
    connection_name             = data['connectionStatus']['name']
    connection_group_id         = data['connectionStatus']['groupId']
    connection_queue_count      = data['connectionStatus']['aggregateSnapshot']['queued']
    connection_flowfiles_in     = data['connectionStatus']['aggregateSnapshot']['flowFilesIn']
    connection_flowfiles_out    = data['connectionStatus']['aggregateSnapshot']['flowFilesOut']
    connection_pattern          = str(data['connectionStatus']['sourceName']) + ' >> ' + str(connection_name) + ' >> ' + str(data['connectionStatus']['destinationName'])
    print '\n'
    print '#########################################################################'
    print '[ INFO ] Connection Details:'
    print '#########################################################################'
    print '[ INFO ] Connection ID:          ' + str(connection_id)
    print '[ INFO ] Connection Name:        ' + str(connection_name)
    print '[ INFO ] Processor GroupID:      ' + str(connection_group_id)
    print '[ INFO ] FlowFiles Queued:       ' + str(connection_queue_count)
    print '[ INFO ] FlowFiles In:           ' + str(connection_flowfiles_in)
    print '[ INFO ] FlowFiles Out:          ' + str(connection_flowfiles_out)
    print '[ INFO ] Source Name:            ' + str(data['connectionStatus']['sourceName'])
    print '[ INFO ] Source ID:              ' + str(data['connectionStatus']['sourceId'])
    print '[ INFO ] Destination Name:       ' + str(data['connectionStatus']['destinationName'])
    print '[ INFO ] Destination ID:         ' + str(data['connectionStatus']['destinationId'])
    return connection_name, connection_group_id, connection_queue_count, connection_flowfiles_in, connection_flowfiles_out, connection_pattern




#ZEND
