/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.loadplugins;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for loading plugins
 *
 * @opensearch.internal
 */
public class LoadPluginsRequest extends ClusterManagerNodeRequest<LoadPluginsRequest> {
    private String pluginPath;
    private String pluginName;
    private boolean refreshAnalysis = false;

    public LoadPluginsRequest() {}

    public LoadPluginsRequest(StreamInput in) throws IOException {
        super(in);
        pluginPath = in.readOptionalString();
        pluginName = in.readOptionalString();
        refreshAnalysis = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(pluginPath);
        out.writeOptionalString(pluginName);
        out.writeBoolean(refreshAnalysis);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    // Getters and setters
    public String getPluginPath() { return pluginPath; }
    public void setPluginPath(String pluginPath) { this.pluginPath = pluginPath; }
    public String getPluginName() { return pluginName; }
    public void setPluginName(String pluginName) { this.pluginName = pluginName; }
    public boolean isRefreshAnalysis() { return refreshAnalysis; }
    public void setRefreshAnalysis(boolean refreshAnalysis) { this.refreshAnalysis = refreshAnalysis; }
}
