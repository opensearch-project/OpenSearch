/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.validatepluginusage;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Request for validating plugin usage before uninstallation
 *
 * @opensearch.internal
 */
public class ValidatePluginUsageRequest extends ClusterManagerNodeRequest<ValidatePluginUsageRequest> {

    private String pluginName;
    private boolean includeUsageDetails;
    private boolean checkDependencies;

    public ValidatePluginUsageRequest() {}

    public ValidatePluginUsageRequest(String pluginName) {
        this.pluginName = pluginName;
        this.includeUsageDetails = true;
        this.checkDependencies = true;
    }

    public ValidatePluginUsageRequest(StreamInput in) throws IOException {
        super(in);
        this.pluginName = in.readString();
        this.includeUsageDetails = in.readBoolean();
        this.checkDependencies = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(pluginName);
        out.writeBoolean(includeUsageDetails);
        out.writeBoolean(checkDependencies);
    }

    public String getPluginName() {
        return pluginName;
    }

    public ValidatePluginUsageRequest setPluginName(String pluginName) {
        this.pluginName = pluginName;
        return this;
    }

    public boolean isIncludeUsageDetails() {
        return includeUsageDetails;
    }

    public ValidatePluginUsageRequest setIncludeUsageDetails(boolean includeUsageDetails) {
        this.includeUsageDetails = includeUsageDetails;
        return this;
    }

    public boolean isCheckDependencies() {
        return checkDependencies;
    }

    public ValidatePluginUsageRequest setCheckDependencies(boolean checkDependencies) {
        this.checkDependencies = checkDependencies;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (pluginName == null || pluginName.trim().isEmpty()) {
            validationException = addValidationError("plugin name is required", validationException);
        }
        return validationException;
    }
}
