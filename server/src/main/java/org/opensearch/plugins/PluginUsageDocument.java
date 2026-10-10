/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import java.time.Instant;

/**
 * Document representing plugin usage information
 *
 * @opensearch.internal
 */
public class PluginUsageDocument {
    
    private String pluginName;
    private String usageType;
    private String resourceName;
    private String indexName;
    private String fieldMapping;
    private String pipelineName;
    private Instant timestamp;
    private String nodeId;
    private boolean active;
    
    public PluginUsageDocument() {}
    
    public String getPluginName() {
        return pluginName;
    }
    
    public PluginUsageDocument setPluginName(String pluginName) {
        this.pluginName = pluginName;
        return this;
    }
    
    public String getUsageType() {
        return usageType;
    }
    
    public PluginUsageDocument setUsageType(String usageType) {
        this.usageType = usageType;
        return this;
    }
    
    public String getResourceName() {
        return resourceName;
    }
    
    public PluginUsageDocument setResourceName(String resourceName) {
        this.resourceName = resourceName;
        return this;
    }
    
    public String getIndexName() {
        return indexName;
    }
    
    public PluginUsageDocument setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }
    
    public String getFieldMapping() {
        return fieldMapping;
    }
    
    public PluginUsageDocument setFieldMapping(String fieldMapping) {
        this.fieldMapping = fieldMapping;
        return this;
    }
    
    public String getPipelineName() {
        return pipelineName;
    }
    
    public PluginUsageDocument setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
        return this;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public PluginUsageDocument setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
        return this;
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public PluginUsageDocument setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }
    
    public boolean isActive() {
        return active;
    }
    
    public PluginUsageDocument setActive(boolean active) {
        this.active = active;
        return this;
    }
}
