#!/usr/bin/env bash

# Recreates the v_nodes_http.json files in this directory. This is
# meant to be an "every once in a while" thing that we do only when
# we want to add a new version of OpenSearch or configure the
# nodes differently. That is why we don't do this in gradle. It also
# allows us to play fast and loose with error handling. If something
# goes wrong you have to manually clean up which is good because it
# leaves around the kinds of things that we need to debug the failure.

# I built this file so the next time I have to regenerate these
# v_nodes_http.json files I won't have to reconfigure OpenSearch
# from scratch. While I was at it I took the time to make sure that
# when we do rebuild the files they don't jump around too much. That
# way the diffs are smaller.

set -e

script_path="$( cd "$(dirname "$0")" ; pwd -P )"
work=$(mktemp -d)
pushd ${work} >> /dev/null
echo Working in ${work}

wget https://artifacts-no-kpi.elastic.co/downloads/elasticsearch/elasticsearch-oss-7.10.2-linux-x86_64.tar.gz
sha512sum -c - << __SHAs
4933c8291e42bc11422176aeb8655a97727553ef6ec55f123c69bbee3c0dc7453f06ac8606a86efd0a6cd2b2c33f7f2615e793e4ad648c68718d71564cda6bbc  elasticsearch-oss-7.10.2-linux-x86_64.tar.gz
__SHAs


function do_version() {
    local version=$1
    local nodes='m1 m2 m3 d1 d2 d3 c1 c2'
    rm -rf ${version}
    mkdir -p ${version}
    pushd ${version} >> /dev/null
    pwd

    tar xf ../elasticsearch-oss-${version}-linux-x86_64.tar.gz
    local http_port=9200
    for node in ${nodes}; do
        mkdir ${node}
        cp -r elasticsearch-${version}/* ${node}
        local master=$([[ "$node" =~ ^m.* ]] && echo 'master,' || echo '')
        local data=$([[ "$node" =~ ^d.* ]] && echo 'data,' || echo '')
        # m2 is always master and data for these test just so we have a node like that
        data=$([[ "$node" == 'm2' ]] && echo 'data,' || echo ${data})
        local transport_port=$((http_port+100))

        cat >> ${node}/config/elasticsearch.yml << __ES_YML
node.name:          ${node}
node.roles:         [${master} ${data} ingest]
node.attr.dummy:    everyone_has_me
node.attr.number:   ${node:1}
node.attr.array:    [${node:0:1}, ${node:1}]
http.port:          ${http_port}
transport.tcp.port: ${transport_port}
cluster.initial_master_nodes: [m1, m2, m3]
discovery.seed_hosts: ['localhost:9300','localhost:9301','localhost:9302']
__ES_YML

        # configure the JVM heap size
        perl -pi -e 's/-Xm([sx]).+/-Xm${1}512m/g' ${node}/config/jvm.options

        echo "starting ${version}/${node}..."
        ${node}/bin/elasticsearch -d -p pidfile

        ((http_port++))
    done

    echo "waiting for cluster to form"
    # got to wait for all the nodes
    until curl -s localhost:9200; do
        sleep .25
    done

    echo "waiting for all nodes to join"
    until [ $(echo ${nodes} | wc -w) -eq $(curl -s localhost:9200/_cat/nodes | wc -l) ]; do
        sleep .25
    done

    # jq sorts the nodes by their http host so the file doesn't jump around when we regenerate it
    curl -s localhost:9200/_nodes/http?pretty \
        | jq '[to_entries[] | ( select(.key == "nodes").value|to_entries|sort_by(.value.http.publish_address)|from_entries|{"key": "nodes", "value": .} ) // .] | from_entries' \
        > ${script_path}/es${version}_nodes_http.json

    for node in ${nodes}; do
        echo "stopping ${version}/${node}..."
        kill $(cat ${node}/pidfile)
    done

    popd >> /dev/null
}

JAVA_HOME=$JAVA11_HOME do_version 7.10.2

popd >> /dev/null
rm -rf ${work}
