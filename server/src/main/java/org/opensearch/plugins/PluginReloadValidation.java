/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import java.util.List;

/**
 * Validation result for plugin reload operations
 *
 * @opensearch.internal
 */
public class PluginReloadValidation {
    
    private String pluginName;
    private boolean safeToReload;
    private List<PluginUsageDocument> activeUsages;
    private List<String> dependentPlugins;
    private ReloadImpactAssessment impactAssessment;
    private List<String> recommendations;
    private String errorMessage;
    
    public PluginReloadValidation() {}
    
    public String getPluginName() {
        return pluginName;
    }
    
    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }
    
    public boolean isSafeToReload() {
        return safeToReload;
    }
    
    public void setSafeToReload(boolean safeToReload) {
        this.safeToReload = safeToReload;
    }
    
    public List<PluginUsageDocument> getActiveUsages() {
        return activeUsages;
    }
    
    public void setActiveUsages(List<PluginUsageDocument> activeUsages) {
        this.activeUsages = activeUsages;
    }
    
    public List<String> getDependentPlugins() {
        return dependentPlugins;
    }
    
    public void setDependentPlugins(List<String> dependentPlugins) {
        this.dependentPlugins = dependentPlugins;
    }
    
    public ReloadImpactAssessment getImpactAssessment() {
        return impactAssessment;
    }
    
    public void setImpactAssessment(ReloadImpactAssessment impactAssessment) {
        this.impactAssessment = impactAssessment;
    }
    
    public List<String> getRecommendations() {
        return recommendations;
    }
    
    public void setRecommendations(List<String> recommendations) {
        this.recommendations = recommendations;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
