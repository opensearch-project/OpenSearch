/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Statistics about plugin usage across the cluster
 *
 * @opensearch.internal
 */
public class PluginUsageStats {
    
    private String pluginName;
    private int totalUsages;
    private Map<String, Integer> usageBreakdown;
    private List<String> affectedIndices;
    private List<String> affectedPipelines;
    private Instant lastUpdated;
    
    public PluginUsageStats() {}
    
    public String getPluginName() {
        return pluginName;
    }
    
    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }
    
    public int getTotalUsages() {
        return totalUsages;
    }
    
    public void setTotalUsages(int totalUsages) {
        this.totalUsages = totalUsages;
    }
    
    public Map<String, Integer> getUsageBreakdown() {
        return usageBreakdown;
    }
    
    public void setUsageBreakdown(Map<String, Integer> usageBreakdown) {
        this.usageBreakdown = usageBreakdown;
    }
    
    public List<String> getAffectedIndices() {
        return affectedIndices;
    }
    
    public void setAffectedIndices(List<String> affectedIndices) {
        this.affectedIndices = affectedIndices;
    }
    
    public List<String> getAffectedPipelines() {
        return affectedPipelines;
    }
    
    public void setAffectedPipelines(List<String> affectedPipelines) {
        this.affectedPipelines = affectedPipelines;
    }
    
    public Instant getLastUpdated() {
        return lastUpdated;
    }
    
    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
