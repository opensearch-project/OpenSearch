# OpenSearch 2.0 SSL Module Design

**PLEASE NOTE: THIS DOCUMENT IS WORK IN PROGRESS AND DOES NOT REPRESENT THE FINAL DESIGN.**

## Objective

This module adds SSL encryption support to REST and transport layers in OpenSearch ensuring all communication is encrypted.

## Requirements

1. _SSL default enable, explicit disable_ The disable should be with a static yml setting.
2. New settings should inter-work with security plugin.
3. Support tunable settings to disable SSL.
4. Should provide easy integrations with network plugins.
5. One-click out of box experience for getting started. But it is okay to have configuration steps for enabling production mode.


_*Requires feedback*_

1. Should support all certificate formats supported by security plugin. todo: discuss defaults
2. Should support TLSv1, TLSv1.1, TLSv1.2, TLSv1.3. todo: discuss defaults
3. SSLProvider support - JDK, OpenSSL todo: discuss
4. Support FIPS 140-2 compliant mode todo: discuss

## Design

Let us walk through the SSL module design from OpenSearch process bootstrap timeline perspective, which would help us better understand the different components of SSL module and how they interact with other OpenSearch components.

As OpenSearch process bootstraps on a node, it first creates an instance of `Node` class, which internally creates an PluginsService and a host of other services associated with a Node. The `PluginService` checks for available security plugins before instantiating Security `SSL module`. If the user has configured any external security plugin that overrides the inbuilt SSL module, `PluginService` loads the external security plugin and skips instantiating the SSL Module. If there is no external security plugin the SSL module gets loaded from the `modules/` directory. Now the SSL module initialization flow is executed, which initializes the SSL Configurations for HTTP and Transport layers. It reads certificates, adds SSLHandler(s) to the netty channels, creates HTTP and transport interceptors. Netty SSLHandler(s) are used to provide ssl en/decryption support for all communication. These initialized SSL configurations will be used during node start for creating the TLS enabled channels for client to node and node to node communication.


```
  --------------------------------


      +---------------------+
      | Inbound Handler  N  |
      +----------+----------+
                /|\
                 |
      +----------+----------+
      | Inbound Handler N-1 |
      +----------+----------+
                /|\
                 .
   ChannelHandlerContext.fireIN_EVT()
          [ method call]
                 .
                 .
      +----------+----------+
      | Inbound Handler  2  |
      +----------+----------+
                /|\
                 |
      +----------+----------+
      |  Server SSL Handler |
      +----------+----------+
                /|\
  ---------------+----------------
                 |
  ---------------+----------------
                 |
         [ Socket.read() ]

    Netty Internal I/O Threads
  --------------------------------
```

Once the instantiation completes, the node is started. As part of node start, all node services, like IndicesService, SnapshotsService, etc. are started as well. The SSL module, which subscribes to the nodeStart event, creates a `.ssl` system index that holds all configurations related to SSL module (see figure 2). It provides configurable hooks to plug-in cross-cluster logic for ssl encryption.

*(#todo - define how bwc works w/ security plugin system index)*

*(#todo - define configurations that are stored in .ssl index)*


**Figure**: REST Request SSL termination flow

**Figure**: Transport Request SSL termination flow

**Figure**: Dashboards to OpenSearch request flow


## New APIs & Interfaces

todo

## New OpenSearch Settings

|	|SETTING NAME	|DEFAULTS	|DESCRIPTION	|
|---	|---	|---	|---	|
|	|`plugins.security.ssl.http.enabled`	|TRUE	|	|
|	|`plugins.security.ssl.transport.enabled`	|TRUE	|	|
|	|	|	|	|
|	|	|	|	|
|	|	|	|	|

## Backward compatibility with OpenSearch 1.x and Elasticsearch 7.x

Figure: SSL Dual Mode

## Certificate and Secrets Management

todo

## Inter-working with Security Plugin

todo

## User experience

This section covers the out of box experience.

This is being tracked separately in [meta issue #1618](https://github.com/opensearch-project/OpenSearch/issues/1618). Welcome your feedback on the issue.

### Local Setup

### Production Setup


### Setup tools










