/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.validatepluginusage;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginReloadValidation;

import java.io.IOException;
import java.util.List;

/**
 * Response for plugin usage validation
 *
 * @opensearch.internal
 */
public class ValidatePluginUsageResponse extends ActionResponse implements ToXContentObject {

    private String pluginName;
    private boolean safeToUnload;
    private int activeUsageCount;
    private List<String> affectedIndices;
    private List<String> affectedPipelines;
    private List<String> dependentPlugins;
    private String riskLevel;
    private List<String> recommendations;
    private String errorMessage;

    public ValidatePluginUsageResponse() {}

    public ValidatePluginUsageResponse(StreamInput in) throws IOException {
        super(in);
        this.pluginName = in.readString();
        this.safeToUnload = in.readBoolean();
        this.activeUsageCount = in.readInt();
        this.affectedIndices = in.readStringList();
        this.affectedPipelines = in.readStringList();
        this.dependentPlugins = in.readStringList();
        this.riskLevel = in.readString();
        this.recommendations = in.readStringList();
        this.errorMessage = in.readOptionalString();
    }

    public ValidatePluginUsageResponse(PluginReloadValidation validation) {
        this.pluginName = validation.getPluginName();
        this.safeToUnload = validation.isSafeToReload();
        this.activeUsageCount = validation.getActiveUsages() != null ? validation.getActiveUsages().size() : 0;
        this.dependentPlugins = validation.getDependentPlugins();
        this.recommendations = validation.getRecommendations();
        this.errorMessage = validation.getErrorMessage();
        
        if (validation.getImpactAssessment() != null) {
            this.riskLevel = validation.getImpactAssessment().getRiskLevel();
        } else {
            this.riskLevel = "UNKNOWN";
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pluginName);
        out.writeBoolean(safeToUnload);
        out.writeInt(activeUsageCount);
        out.writeStringCollection(affectedIndices != null ? affectedIndices : List.of());
        out.writeStringCollection(affectedPipelines != null ? affectedPipelines : List.of());
        out.writeStringCollection(dependentPlugins != null ? dependentPlugins : List.of());
        out.writeString(riskLevel != null ? riskLevel : "UNKNOWN");
        out.writeStringCollection(recommendations != null ? recommendations : List.of());
        out.writeOptionalString(errorMessage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("plugin_name", pluginName);
        builder.field("safe_to_unload", safeToUnload);
        builder.field("active_usage_count", activeUsageCount);
        builder.field("risk_level", riskLevel);
        
        if (affectedIndices != null && !affectedIndices.isEmpty()) {
            builder.field("affected_indices", affectedIndices);
        }
        
        if (affectedPipelines != null && !affectedPipelines.isEmpty()) {
            builder.field("affected_pipelines", affectedPipelines);
        }
        
        if (dependentPlugins != null && !dependentPlugins.isEmpty()) {
            builder.field("dependent_plugins", dependentPlugins);
        }
        
        if (recommendations != null && !recommendations.isEmpty()) {
            builder.field("recommendations", recommendations);
        }
        
        if (errorMessage != null) {
            builder.field("error", errorMessage);
        }
        
        builder.endObject();
        return builder;
    }

    // Getters and setters
    public String getPluginName() {
        return pluginName;
    }

    public ValidatePluginUsageResponse setPluginName(String pluginName) {
        this.pluginName = pluginName;
        return this;
    }

    public boolean isSafeToUnload() {
        return safeToUnload;
    }

    public ValidatePluginUsageResponse setSafeToUnload(boolean safeToUnload) {
        this.safeToUnload = safeToUnload;
        return this;
    }

    public int getActiveUsageCount() {
        return activeUsageCount;
    }

    public ValidatePluginUsageResponse setActiveUsageCount(int activeUsageCount) {
        this.activeUsageCount = activeUsageCount;
        return this;
    }

    public List<String> getAffectedIndices() {
        return affectedIndices;
    }

    public ValidatePluginUsageResponse setAffectedIndices(List<String> affectedIndices) {
        this.affectedIndices = affectedIndices;
        return this;
    }

    public List<String> getAffectedPipelines() {
        return affectedPipelines;
    }

    public ValidatePluginUsageResponse setAffectedPipelines(List<String> affectedPipelines) {
        this.affectedPipelines = affectedPipelines;
        return this;
    }

    public List<String> getDependentPlugins() {
        return dependentPlugins;
    }

    public ValidatePluginUsageResponse setDependentPlugins(List<String> dependentPlugins) {
        this.dependentPlugins = dependentPlugins;
        return this;
    }

    public String getRiskLevel() {
        return riskLevel;
    }

    public ValidatePluginUsageResponse setRiskLevel(String riskLevel) {
        this.riskLevel = riskLevel;
        return this;
    }

    public List<String> getRecommendations() {
        return recommendations;
    }

    public ValidatePluginUsageResponse setRecommendations(List<String> recommendations) {
        this.recommendations = recommendations;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ValidatePluginUsageResponse setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }
}
