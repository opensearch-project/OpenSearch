# OpenSearch 2.0 SSL Module Design

**PLEASE NOTE: THIS DOCUMENT IS WORK IN PROGRESS AND DOES NOT REPRESENT THE FINAL DESIGN.**

## Objective

This module adds SSL encryption support to REST and transport layers in OpenSearch ensuring all communication is encrypted.

## Requirements

1. _SSL default enable, explicit disable_ The disable should be with a static yml setting.
2. New settings should inter-work with security plugin.
3. Support tunable settings to disable SSL.
4. Should support all certificate formats supported by security plugin. todo
5.

## Design

Figure 1: REST Request SSL termination flow

Figure 2: Transport Request SSL termination flow

Figure 3: Dashboards to OpenSearch request flow


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

### Local Setup

### Production Setup

### Setup tools

## Feedback

todo







