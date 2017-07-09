
############################################################################################
#
#   NiFi APIs (https://nifi.apache.org/docs/nifi-docs/rest-api/)
#
#   This script provides many functions that allow users to monitor NiFi, such as:
#       - General System Diagnostics (heap utilization, content and flowfile storage, etc.)
#       - Detect stopped processors
#
############################################################################################

import sys,re
import requests
import json


nifi_host='http://dzaratsian-hdf0.field.hortonworks.com:9090/'


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
    print '[ INFO ] Heap Usage:           ' + str(heap_pct_used)
    print '[ INFO ] Heap Size:            ' + str(heap_size)
    print '[ INFO ] Content Repo Usage:   ' + str(content_repo_usage)
    print '[ INFO ] Content Repo Size:    ' + str(content_repo_size)
    print '[ INFO ] Flowfile Repo Usage:  ' + str(flowfile_repo_usage)
    print '[ INFO ] Flowfile Repo Size:   ' + str(flowfile_repo_size)
    return data


def flow_status(nifi_host):
    url    = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/status'   
    req    = requests.get(url)
    data   = json.loads(req.content)
    processors_stopped  = data['controllerStatus']['stoppedCount']
    processors_running  = data['controllerStatus']['runningCount']
    queued_flowfiles    = data['controllerStatus']['queued']
    invalid_processors  = data['controllerStatus']['invalidCount']
    active_thread_count = data['controllerStatus']['activeThreadCount']
    print '[ INFO ] Processors Running:   ' + str(processors_stopped)
    print '[ INFO ] Processors Stopped:   ' + str(processors_running)
    print '[ INFO ] Flowfiles in Queue:   ' + str(queued_flowfiles)
    print '[ INFO ] Invalid Processors:   ' + str(invalid_processors)
    print '[ INFO ] Active Thread Count:  ' + str(active_thread_count)
    return data


def list_processors(nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/search-results'   
    req  = requests.get(url)
    data = json.loads(req.content)
    processors = [processor['id'] for processor in data['searchResultsDTO']['processorResults']]
    print '[ INFO ] ' + str(len(processors)) + ' processors found.'
    return data, processors


def list_connections(nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/search-results'   
    req  = requests.get(url)
    data = json.loads(req.content)
    connections = [connection['id'] for connection in data['searchResultsDTO']['connectionResults']]
    print '[ INFO ] ' + str(len(connections)) + ' connections found.'
    return data, connections


def get_processor_status(processor_id, nifi_host):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + '/flow/processors/' + str(processor_id) + '/status'   
    req  = requests.get(url)
    data = json.loads(req.content)
    print '[ INFO ] Processor ID:           ' + str(processor_id)
    print '[ INFO ] Processor Name:         ' + str(data['processorStatus']['name'])
    print '[ INFO ] Processor GroupID:      ' + str(data['processorStatus']['groupId'])
    print '[ INFO ] Processor Run Status:   ' + str(data['processorStatus']['aggregateSnapshot']['runStatus']) 
    return data















def ztest(api='/flow/cluster/summary' ,nifi_host='http://dzaratsian-hdf0.field.hortonworks.com:9090/'):
    url  = re.sub('/$','',nifi_host) + '/nifi-api' + str(api)
    req  = requests.get(url)
    data = json.loads(req.content)
    return data





#ZEND
