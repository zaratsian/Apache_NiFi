
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

import re,sys
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
    print '\n'
    print '#########################################################################'
    print '[ INFO ] NiFi Metadata:'
    print '#########################################################################'
    print '[ INFO ] Total Process Groups:   ' + str(len(list_process_group))
    print '[ INFO ] Total Processors:       ' + str(len(list_processors))
    print '[ INFO ] Total Connections:      ' + str(len(list_connections))
    print '[ INFO ] Total Input Ports:      ' + str(len(list_inputports))
    print '[ INFO ] Total Output Ports:     ' + str(len(list_outputports))
    print '\n'
    return list_process_group, list_processors, list_connections



def nifi_process_group_status(process_group_id, nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/process-groups/' + str(process_group_id) + '/status'   
    req  = requests.get(url)
    data = json.loads(req.content)
    process_group_name           = data['processGroupStatus']['name']
    process_group_queued_count   = data['processGroupStatus']['aggregateSnapshot']['queued']
    process_group_status_message = '''
    #########################################################################
    [ INFO ] Process Group Details:
    #########################################################################
    [ INFO ] Process Group ID:       ''' + str(process_group_id) + '''
    [ INFO ] Process Group Name:     ''' + str(process_group_name)
    return process_group_status_message, process_group_name



def nifi_processor_status(processor_id, nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/processors/' + str(processor_id) + '/status'   
    req  = requests.get(url)
    data = json.loads(req.content)
    processor_name           = data['processorStatus']['name']
    processor_group_id       = data['processorStatus']['groupId']
    processor_runstatus      = data['processorStatus']['aggregateSnapshot']['runStatus']
    processor_status_message = '''
    #########################################################################
    [ INFO ] Processor Details:
    #########################################################################
    [ INFO ] Processor ID:           ''' + str(processor_id) + '''
    [ INFO ] Processor Name:         ''' + str(processor_name) + '''
    [ INFO ] Processor GroupID:      ''' + str(processor_group_id) + '''
    [ INFO ] Processor Run Status:   ''' + str(processor_runstatus) + '''
    [ INFO ] FlowFiles In:           ''' + str(data['processorStatus']['aggregateSnapshot']['flowFilesIn']) + '''
    [ INFO ] FlowFiles Out:          ''' + str(data['processorStatus']['aggregateSnapshot']['flowFilesOut']) + '''
    [ INFO ] Active Thread Count:    ''' + str(data['processorStatus']['aggregateSnapshot']['activeThreadCount'])
    return processor_status_message, processor_id, processor_name, processor_group_id, processor_runstatus



def nifi_connection(connection_id, nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/connections/' + str(connection_id)  
    req  = requests.get(url)
    data = json.loads(req.content)
    conn_name                           = data['status']['name']
    conn_group_id                       = data['status']['groupId']
    conn_queue_count                    = data['status']['aggregateSnapshot']['queued']
    conn_flowfiles_in                   = data['status']['aggregateSnapshot']['flowFilesIn']
    conn_flowfiles_out                  = data['status']['aggregateSnapshot']['flowFilesOut']
    conn_percent_use_count              = data['status']['aggregateSnapshot']['percentUseCount']
    conn_percent_use_bytes              = data['status']['aggregateSnapshot']['percentUseBytes']   
    conn_backpressure_size_threshold    = data['component']['backPressureDataSizeThreshold']
    conn_backpressure_obj_threshold     = data['component']['backPressureObjectThreshold']
    conn_pattern                        = str(data['status']['sourceName']) + ' >> ' + str(conn_name) + ' >> ' + str(data['status']['destinationName'])
    conn_message                        = '''
    #########################################################################
    [ INFO ] Connection Details:
    #########################################################################
    [ INFO ] Connection ID:          ''' + str(connection_id) + '''
    [ INFO ] Connection Name:        ''' + str(conn_name) + '''
    [ INFO ] Processor GroupID:      ''' + str(conn_group_id) + '''
    [ INFO ] FlowFiles Queued:       ''' + str(conn_queue_count) + '''
    [ INFO ] FlowFiles In:           ''' + str(conn_flowfiles_in) + '''
    [ INFO ] FlowFiles Out:          ''' + str(conn_flowfiles_out) + '''
    [ INFO ] Source Name:            ''' + str(data['status']['sourceName']) + '''
    [ INFO ] Source ID:              ''' + str(data['status']['sourceId']) + '''
    [ INFO ] Destination Name:       ''' + str(data['status']['destinationName']) + '''
    [ INFO ] Destination ID:         ''' + str(data['status']['destinationId'])
    return conn_message, conn_name, conn_group_id, conn_queue_count, conn_flowfiles_in, conn_flowfiles_out, conn_percent_use_count, conn_percent_use_bytes, conn_pattern



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
    connection_status_message   = '''
    #########################################################################
    [ INFO ] Connection Details:
    #########################################################################
    [ INFO ] Connection ID:          ''' + str(connection_id) + '''
    [ INFO ] Connection Name:        ''' + str(connection_name) + '''
    [ INFO ] Processor GroupID:      ''' + str(connection_group_id) + '''
    [ INFO ] FlowFiles Queued:       ''' + str(connection_queue_count) + '''
    [ INFO ] FlowFiles In:           ''' + str(connection_flowfiles_in) + '''
    [ INFO ] FlowFiles Out:          ''' + str(connection_flowfiles_out) + '''
    [ INFO ] Source Name:            ''' + str(data['connectionStatus']['sourceName']) + '''
    [ INFO ] Source ID:              ''' + str(data['connectionStatus']['sourceId']) + '''
    [ INFO ] Destination Name:       ''' + str(data['connectionStatus']['destinationName']) + '''
    [ INFO ] Destination ID:         ''' + str(data['connectionStatus']['destinationId'])
    return connection_status_message, connection_name, connection_group_id, connection_queue_count, connection_flowfiles_in, connection_flowfiles_out, connection_pattern



if __name__ == '__main__':
    
    list_process_group, list_processors, list_connections = nifi_metadata(nifi_host)
    
    print '#########################################################################'
    print '[ INFO ] Detecting NiFi Issues, then printing results...'
    print '#########################################################################'
    
    # Flag any processors that are not running
    for processor_id in list_processors:
        processor_status_message, processor_id, processor_name, processor_group_id, processor_runstatus = nifi_processor_status(processor_id, nifi_host)
        process_group_status_message, process_group_name = nifi_process_group_status(processor_group_id, nifi_host)
        if processor_runstatus != 'Running':
            print '[ WARNING ] Processor ' + str(processor_name) + ' has a status of ' + str(processor_runstatus) + ' (Process Group: ' + str(process_group_name) + ')' 
    
    # Flag any connection that has a high queue count (either the bytes or count is queud over X%)
    for connection_id in list_connections:
        conn_message, conn_name, conn_group_id, conn_queue_count, conn_flowfiles_in, conn_flowfiles_out, conn_percent_use_count, conn_percent_use_bytes, conn_pattern = nifi_connection(connection_id, nifi_host)
        process_group_status_message, process_group_name = nifi_process_group_status(conn_group_id, nifi_host)
        if (conn_percent_use_count >= 80) or (conn_percent_use_bytes >= 80):
            print '[ WARNING ] Connection ' + str(conn_name) + ' is at or over 80% capacity (Process Group: ' + str(conn_group_id) + ')'



#ZEND
