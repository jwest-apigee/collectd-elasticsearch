import collectd
import json
import urllib2
import socket
import collections
import requests

PREFIX = "elasticsearch"
ES_CLUSTER = "elasticsearch"
ES_HOST = "localhost"
ES_PORT = 9200
ES_VERSION = "1.0"
ES_URL = ""
VERBOSE_LOGGING = True

Stat = collections.namedtuple('Stat', ('type', 'path'))

STATS_CUR = {}

# DICT: ElasticSearch 1.0.0
STATS_ES1 = {
    ## STORE
    'indices.store.throttle-time': Stat("counter", "nodes.%s.indices.store.throttle_time_in_millis"),

    ##SEARCH
    'indices.search.open-contexts': Stat("gauge", "nodes.%s.indices.search.open_contexts"),

    ##CACHE
    'indices.cache.field.eviction': Stat("counter", "nodes.%s.indices.fielddata.evictions"),
    'indices.cache.field.size': Stat("bytes", "nodes.%s.indices.fielddata.memory_size_in_bytes"),
    'indices.cache.filter.evictions': Stat("counter", "nodes.%s.indices.filter_cache.evictions"),
    'indices.cache.filter.size': Stat("bytes", "nodes.%s.indices.filter_cache.memory_size_in_bytes"),

    ##GC
    'jvm.gc.time': Stat("counter", "nodes.%s.jvm.gc.collectors.young.collection_time_in_millis"),
    'jvm.gc.count': Stat("counter", "nodes.%s.jvm.gc.collectors.young.collection_count"),
    'jvm.gc.old-time': Stat("counter", "nodes.%s.jvm.gc.collectors.old.collection_time_in_millis"),
    'jvm.gc.old-count': Stat("counter", "nodes.%s.jvm.gc.collectors.old.collection_count"),

    #JEFF
    'jvm.gc.young-count': Stat("counter", "nodes.%s.jvm.gc.collectors.young.collection_count"),
    'jvm.gc.young-time': Stat("counter", "nodes.%s.jvm.gc.collectors.young.collection_time_in_millis"),

    'jvm.pools.young-used': Stat("counter", "nodes.%s.jvm.mem.pools.young.used_in_bytes"),
    'jvm.pools.young-max': Stat("counter", "nodes.%s.jvm.mem.pools.young.max_in_bytes"),
    'jvm.pools.survivor-used': Stat("counter", "nodes.%s.jvm.mem.pools.survivor.used_in_bytes"),
    'jvm.pools.survivor-max': Stat("counter", "nodes.%s.jvm.mem.pools.survivor.max_in_bytes"),
    'jvm.pools.old-used': Stat("counter", "nodes.%s.jvm.mem.pools.old.used_in_bytes"),
    'jvm.pools.old-max': Stat("counter", "nodes.%s.jvm.mem.pools.old.max_in_bytes"),

    'jvm.threads.count': Stat("counter", "nodes.%s.jvm.threads.count"),
    'jvm.threads.peak_count': Stat("counter", "nodes.%s.jvm.threads.peak_count"),


    ## FLUSH
    'indices.flush.total': Stat("counter", "nodes.%s.indices.flush.total"),
    'indices.flush.time': Stat("counter", "nodes.%s.indices.flush.total_time_in_millis"),

    ## MERGES
    'indices.merges.current': Stat("gauge", "nodes.%s.indices.merges.current"),
    'indices.merges.current-docs': Stat("gauge", "nodes.%s.indices.merges.current_docs"),
    'indices.merges.current-size': Stat("bytes", "nodes.%s.indices.merges.current_size_in_bytes"),
    'indices.merges.total': Stat("counter", "nodes.%s.indices.merges.total"),
    'indices.merges.total-docs': Stat("gauge", "nodes.%s.indices.merges.total_docs"),
    'indices.merges.total-size': Stat("bytes", "nodes.%s.indices.merges.total_size_in_bytes"),
    'indices.merges.time': Stat("counter", "nodes.%s.indices.merges.total_time_in_millis"),

    ## REFRESH
    'indices.refresh.total': Stat("counter", "nodes.%s.indices.refresh.total"),
    'indices.refresh.time': Stat("counter", "nodes.%s.indices.refresh.total_time_in_millis"),
}

# DICT: ElasticSearch 0.9.x
STATS_ES09 = {

    ##GC
    'jvm.gc.time': Stat("counter", "nodes.%s.jvm.gc.collection_time_in_millis"),
    'jvm.gc.count': Stat("counter", "nodes.%s.jvm.gc.collection_count"),

    ##CPU
    'process.cpu.percent': Stat("gauge", "nodes.%s.process.cpu.percent"),
}

# DICT: Common stuff
STATS = {

    ## DOCS
    'indices.docs.count': Stat("gauge", "nodes.%s.indices.docs.count"),
    'indices.docs.deleted': Stat("counter", "nodes.%s.indices.docs.deleted"),

    ## STORE
    'indices.store.size': Stat("bytes", "nodes.%s.indices.store.size_in_bytes"),

    ## INDEXING
    'indices.indexing.index-total': Stat("counter", "nodes.%s.indices.indexing.index_total"),
    'indices.indexing.index-time': Stat("counter", "nodes.%s.indices.indexing.index_time_in_millis"),
    'indices.indexing.delete-total': Stat("counter", "nodes.%s.indices.indexing.delete_total"),
    'indices.indexing.delete-time': Stat("counter", "nodes.%s.indices.indexing.delete_time_in_millis"),
    'indices.indexing.index-current': Stat("gauge", "nodes.%s.indices.indexing.index_current"),
    'indices.indexing.delete-current': Stat("gauge", "nodes.%s.indices.indexing.delete_current"),

    ## GET
    'indices.get.total': Stat("counter", "nodes.%s.indices.get.total"),
    'indices.get.time': Stat("counter", "nodes.%s.indices.get.time_in_millis"),
    'indices.get.exists-total': Stat("counter", "nodes.%s.indices.get.exists_total"),
    'indices.get.exists-time': Stat("counter", "nodes.%s.indices.get.exists_time_in_millis"),
    'indices.get.missing-total': Stat("counter", "nodes.%s.indices.get.missing_total"),
    'indices.get.missing-time': Stat("counter", "nodes.%s.indices.get.missing_time_in_millis"),
    'indices.get.current': Stat("gauge", "nodes.%s.indices.get.current"),

    ## SEARCH
    'indices.search.query-current': Stat("gauge", "nodes.%s.indices.search.query_current"),
    'indices.search.query-total': Stat("counter", "nodes.%s.indices.search.query_total"),
    'indices.search.query-time': Stat("counter", "nodes.%s.indices.search.query_time_in_millis"),
    'indices.search.fetch-current': Stat("gauge", "nodes.%s.indices.search.fetch_current"),
    'indices.search.fetch-total': Stat("counter", "nodes.%s.indices.search.fetch_total"),
    'indices.search.fetch-time': Stat("counter", "nodes.%s.indices.search.fetch_time_in_millis"),

    # JVM METRICS #
    ## MEM
    'jvm.mem.heap-committed': Stat("bytes", "nodes.%s.jvm.mem.heap_committed_in_bytes"),
    'jvm.mem.heap-used': Stat("bytes", "nodes.%s.jvm.mem.heap_used_in_bytes"),
    'jvm.mem.heap-used-percent': Stat("percent", "nodes.%s.jvm.mem.heap_used_percent"),
    'jvm.mem.non-heap-committed': Stat("bytes", "nodes.%s.jvm.mem.non_heap_committed_in_bytes"),
    'jvm.mem.non-heap-used': Stat("bytes", "nodes.%s.jvm.mem.non_heap_used_in_bytes"),

    ## THREADS
    'jvm.threads.count': Stat("gauge", "nodes.%s.jvm.threads.count"),
    'jvm.threads.peak': Stat("gauge", "nodes.%s.jvm.threads.peak_count"),

    # TRANSPORT METRICS #
    'transport.server_open': Stat("gauge", "nodes.%s.transport.server_open"),
    'transport.rx.count': Stat("counter", "nodes.%s.transport.rx_count"),
    'transport.rx.size': Stat("bytes", "nodes.%s.transport.rx_size_in_bytes"),
    'transport.tx.count': Stat("counter", "nodes.%s.transport.tx_count"),
    'transport.tx.size': Stat("bytes", "nodes.%s.transport.tx_size_in_bytes"),

    # HTTP METRICS #
    'http.current_open': Stat("gauge", "nodes.%s.http.current_open"),
    'http.total_open': Stat("counter", "nodes.%s.http.total_opened"),

    # PROCESS METRICS #
    'process.open_file_descriptors': Stat("gauge", "nodes.%s.process.open_file_descriptors"),
}


# FUNCTION: Collect stats from JSON result
def lookup_stat(stat, json):

    node = json['nodes'].keys()[0]
    val = dig_it_up(json, STATS_CUR[stat].path % node)

    # Check to make sure we have a valid result
    # dig_it_up returns False if no match found
    if not isinstance(val, bool):
        return int(val)
    else:
        return None


def configure_callback(conf):
    """Received configuration information"""
    global ES_HOST, ES_PORT, ES_URL, ES_VERSION, VERBOSE_LOGGING, STATS_CUR

    for node in conf.children:
        if node.key == 'Host':
            ES_HOST = node.values[0]
        elif node.key == 'Port':
            ES_PORT = int(node.values[0])
        elif node.key == 'Verbose':
            VERBOSE_LOGGING = bool(node.values[0])
        elif node.key == 'Cluster':
            ES_CLUSTER = node.values[0]
        elif node.key == "Version":
            ES_VERSION = node.values[0]
        else:
            collectd.warning('elasticsearch plugin: Unknown config key: %s.'
                             % node.key)
    if ES_VERSION == "1.0":
        ES_URL = "http://" + ES_HOST + ":" + str(ES_PORT) + "/_nodes/_local/stats/transport,http,process,jvm,indices,thread_pool"
        STATS_CUR = dict(STATS.items() + STATS_ES1.items())
    else:
        ES_URL = "http://" + ES_HOST + ":" + str(ES_PORT) + "/_cluster/nodes/_local/stats?http=true&process=true&jvm=true&transport=true&thread_pool=true"
        STATS_CUR = dict(STATS.items() + STATS_ES09.items())

    # add info on thread pools
    for pool in ['generic', 'index', 'get', 'snapshot', 'merge', 'optimize', 'bulk', 'warmer', 'flush', 'search', 'refresh']:
        for attr in ['threads', 'queue', 'active', 'largest']:
            path = 'thread_pool.{0}.{1}'.format(pool, attr)
            STATS_CUR[path] = Stat("gauge", 'nodes.%s.{0}'.format(path))
        for attr in ['completed', 'rejected']:
            path = 'thread_pool.{0}.{1}'.format(pool, attr)
            STATS_CUR[path] = Stat("counter", 'nodes.%s.{0}'.format(path))

    log_verbose('Configured with version=%s, host=%s, port=%s, url=%s' % (ES_VERSION, ES_HOST, ES_PORT, ES_URL))


def fetch_stats():
    global ES_CLUSTER

    try:
        result = json.load(urllib2.urlopen(ES_URL, timeout=10))
    except urllib2.URLError, e:
        collectd.error('elasticsearch plugin: Error connecting to %s - %r' % (ES_URL, e))
        return None
    print result['cluster_name']

    ES_CLUSTER = result['cluster_name']
    return parse_stats(result)


def parse_stats(json):
    """Parse stats response from ElasticSearch"""
    for name, key in STATS_CUR.iteritems():
        result = lookup_stat(name, json)
        dispatch_stat(result, name, key)


def dispatch_stat(result, name, key):
    """Read a key from info response data and dispatch a value"""
    if result is None:
        collectd.warning('elasticsearch plugin: Value not found for %s' % name)
        return

    estype = key.type
    value = int(result)
    log_verbose('Sending value[%s]: %s=%s' % (estype, name, value))

    # val = collectd.Values(plugin='elasticsearch')
    # val.plugin_instance = ES_CLUSTER
    # val.type = estype
    # val.type_instance = name
    # val.values = [value]
    # val.dispatch()


def dig_it_up(obj, path):
    try:
        if type(path) in (str, unicode):
            path = path.split('.')
        return reduce(lambda x, y: x[y], path, obj)
    except:
        return False


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return

    print 'elasticsearch plugin [verbose]: %s' % msg



ES_URL = "http://" + ES_HOST + ":" + str(ES_PORT) + "/_nodes/_local/stats/transport,http,process,jvm,indices,thread_pool"
STATS_CUR = dict(STATS.items() + STATS_ES1.items())

# add info on thread pools
for pool in ['generic', 'index', 'get', 'snapshot', 'merge', 'optimize', 'bulk', 'warmer', 'flush', 'search', 'refresh']:
    for attr in ['threads', 'queue', 'active', 'largest']:
        path = 'thread_pool.{0}.{1}'.format(pool, attr)
        STATS_CUR[path] = Stat("gauge", 'nodes.%s.{0}'.format(path))
    for attr in ['completed', 'rejected']:
        path = 'thread_pool.{0}.{1}'.format(pool, attr)
        STATS_CUR[path] = Stat("counter", 'nodes.%s.{0}'.format(path))


r = requests.get(ES_URL)
response = r.json()

parse_stats(response)