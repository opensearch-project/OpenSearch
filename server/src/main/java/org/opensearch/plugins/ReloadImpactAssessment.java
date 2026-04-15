/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

/**
 * Assessment of the impact of reloading a plugin
 *
 * @opensearch.internal
 */
public class ReloadImpactAssessment {
    
    private long searchImpactedIndices;
    private long ingestionImpactedPipelines;
    private String riskLevel;
    
    public ReloadImpactAssessment() {}
    
    public long getSearchImpactedIndices() {
        return searchImpactedIndices;
    }
    
    public void setSearchImpactedIndices(long searchImpactedIndices) {
        this.searchImpactedIndices = searchImpactedIndices;
    }
    
    public long getIngestionImpactedPipelines() {
        return ingestionImpactedPipelines;
    }
    
    public void setIngestionImpactedPipelines(long ingestionImpactedPipelines) {
        this.ingestionImpactedPipelines = ingestionImpactedPipelines;
    }
    
    public String getRiskLevel() {
        return riskLevel;
    }
    
    public void setRiskLevel(String riskLevel) {
        this.riskLevel = riskLevel;
    }
}
