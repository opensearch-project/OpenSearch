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

set -e -o pipefail

script_path="$( cd "$(dirname "$0")" ; pwd -P )"
work=$(mktemp -d)
pushd ${work} >> /dev/null
echo Working in ${work}

wget https://artifacts.opensearch.org/releases/core/opensearch/1.0.0/opensearch-min-1.0.0-linux-x64.tar.gz
wget https://artifacts.opensearch.org/releases/core/opensearch/2.0.0/opensearch-min-2.0.0-linux-x64.tar.gz
sha512sum -c - << __SHAs
96595cd3b173188d8a3f0f18d7bfa2457782839d06b519f01a99b4dc0280f81b08ba1d01bd1aef454feaa574cbbd04d3ad9a1f6a829182627e914f3e58f2899f opensearch-min-1.0.0-linux-x64.tar.gz
5b91456a2eb517bc48f13bec0a3f9c220494bd5fe979946dce6cfc3fa7ca00b003927157194d62f2a1c36c850eda74c70b93fbffa91bb082b2e1a17985d50976 opensearch-min-2.0.0-linux-x64.tar.gz
__SHAs


function do_version() {
    local version=$1
    local nodes='m1 m2 m3 d1 d2 d3 c1 c2'
    rm -rf ${version}
    mkdir -p ${version}
    pushd ${version} >> /dev/null

    tar xf ../opensearch-min-${version}-linux-x64.tar.gz
    local http_port=9200
    for node in ${nodes}; do
        mkdir ${node}
        cp -r opensearch-${version}/* ${node}
        local cluster_manager=$([[ "$node" =~ ^m.* ]] && echo 'cluster_manager,' || echo '')
        # 'cluster_manager' role is add in version 2.x and above, use 'master' role in 1.x
        cluster_manager=$([[ ! "$cluster_manager" == '' && ${version} =~ ^1\. ]] && echo 'master,' || echo ${cluster_manager})
        local data=$([[ "$node" =~ ^d.* ]] && echo 'data,' || echo '')
        # m2 is always cluster_manager and data for these test just so we have a node like that
        data=$([[ "$node" == 'm2' ]] && echo 'data,' || echo ${data})
        # setting name 'cluster.initial_cluster_manager_nodes' is add in version 2.x and above
        local initial_cluster_manager_nodes=$([[ ${version} =~ ^1\. ]] && echo 'initial_master_nodes' || echo 'initial_cluster_manager_nodes')
        local transport_port=$((http_port+100))

        cat >> ${node}/config/opensearch.yml << __OPENSEARCH_YML
node.name:          ${node}
node.roles:         [${cluster_manager} ${data} ingest]
node.attr.dummy:    everyone_has_me
node.attr.number:   ${node:1}
node.attr.array:    [${node:0:1}, ${node:1}]
http.port:          ${http_port}
transport.tcp.port: ${transport_port}
cluster.${initial_cluster_manager_nodes}: [m1, m2, m3]
discovery.seed_hosts: ['localhost:9300','localhost:9301','localhost:9302']
__OPENSEARCH_YML

        # configure the JVM heap size
        perl -pi -e 's/-Xm([sx]).+/-Xm${1}512m/g' ${node}/config/jvm.options

        echo "starting ${version}/${node}..."
        ${node}/bin/opensearch -d -p pidfile

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
        > ${script_path}/${version}_nodes_http.json

    for node in ${nodes}; do
        echo "stopping ${version}/${node}..."
        kill $(cat ${node}/pidfile)
    done

    popd >> /dev/null
}

JAVA_HOME=$JAVA11_HOME do_version 1.0.0
JAVA_HOME=$JAVA11_HOME do_version 2.0.0

popd >> /dev/null
rm -rf ${work}
