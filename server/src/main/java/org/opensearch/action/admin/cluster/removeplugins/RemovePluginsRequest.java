/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.removeplugins;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Request for removing plugins
 *
 * @opensearch.internal
 */
public class RemovePluginsRequest extends ClusterManagerNodeRequest<RemovePluginsRequest> {
    private String pluginName;
    private List<String> pluginNames;
    private boolean refreshAnalysis = false;
    private boolean forceRemove = false;

    public RemovePluginsRequest() {}

    public RemovePluginsRequest(StreamInput in) throws IOException {
        super(in);
        pluginName = in.readOptionalString();
        pluginNames = in.readOptionalStringList();
        refreshAnalysis = in.readBoolean();
        forceRemove = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(pluginName);
        out.writeOptionalStringCollection(pluginNames);
        out.writeBoolean(refreshAnalysis);
        out.writeBoolean(forceRemove);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    // Getters and setters
    public String getPluginName() { return pluginName; }
    public void setPluginName(String pluginName) { this.pluginName = pluginName; }
    public List<String> getPluginNames() { return pluginNames; }
    public void setPluginNames(List<String> pluginNames) { this.pluginNames = pluginNames; }
    public boolean isRefreshAnalysis() { return refreshAnalysis; }
    public void setRefreshAnalysis(boolean refreshAnalysis) { this.refreshAnalysis = refreshAnalysis; }
    public boolean isForceRemove() { return forceRemove; }
    public void setForceRemove(boolean forceRemove) { this.forceRemove = forceRemove; }
}
