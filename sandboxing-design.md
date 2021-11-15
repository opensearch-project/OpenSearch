# OpenSearch Plugin Sandboxing Design

**PLEASE NOTE: THIS DOCUMENT IS WORK IN PROGRESS AND DOES NOT REPRESENT THE FINAL DESIGN.**

## Objective

This feature enables any plugin to run safely without impacting the cluster and the system.

## Problem

Plugin architecture enables extending core features of OpenSearch. There are various kinds of plugins which are supported.
But, the architecture has significant problems for OpenSearch customers. Mainly, plugins can fatally impact the cluster
e.g  critical workloads like ingestion/search traffic would be impacted because of a non-critical plugin like s3-repository failed with an exception.
The problem multiplies exponentially when we would like to run an arbitrary plugin as OpenSearch core and system resources are not protected well enough.

Zooming in technically, Plugins run with-in the same process as OpenSearch. As OpenSearch process is bootstrapping, it initializes [PluginService.java](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/plugins/PluginsService.java#L124) via
[Node.java](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/node/Node.java#L392). All plugins are classloaded via [loadPlugin](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/plugins/PluginsService.java#L765:20) during the bootstrap of PluginService.
It looks for `plugins` directory and loads the classpath where all the plugin jar and its dependencies are already present. During the bootstrap, each plugin is initialized and they do have various interfaces through which they could choose to subscribe to state changes within the cluster e.g [ClusterService.java](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/cluster/service/ClusterService.java).

Resources on the system for Plugins in OpenSearch are managed via Java Security Manager. It is initialized during the [bootstrap](https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/bootstrap/OpenSearch.java#L91) of OpenSearch process.
Each plugin defines a `security.policy` file e.g [Anomaly Detection Plugin](https://github.com/opensearch-project/anomaly-detection/blob/main/src/main/plugin-metadata/plugin-security.policy#L6)

As we can see, plugins are loaded into OpenSearch process which fundamentally needs to change.

## Design

TBD

### Plugin Developer experience

TBD

## Feedback

TBD
