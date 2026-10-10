/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginInfoWithStatus;
import org.opensearch.plugins.PluginLoadStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Information about plugins and modules with load status information for hot reload functionality
 *
 * @opensearch.internal
 */
public class PluginsAndModulesWithStatus extends PluginsAndModules {
    private final Map<String, PluginLoadStatus> pluginLoadStatusMap;

    public PluginsAndModulesWithStatus(List<PluginInfo> plugins, List<PluginInfo> modules, Map<String, PluginLoadStatus> pluginLoadStatusMap) {
        super(plugins, modules);
        this.pluginLoadStatusMap = pluginLoadStatusMap;
    }

    public PluginsAndModulesWithStatus(StreamInput in) throws IOException {
        super(in);
        this.pluginLoadStatusMap = in.readMap(StreamInput::readString, stream -> PluginLoadStatus.valueOf(stream.readString()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(pluginLoadStatusMap, StreamOutput::writeString, (stream, status) -> stream.writeString(status.name()));
    }

    /**
     * Returns an ordered list of plugins with their load status information
     */
    public List<PluginInfoWithStatus> getPluginInfosWithStatus() {
        List<PluginInfoWithStatus> pluginsWithStatus = new ArrayList<>();
        for (PluginInfo pluginInfo : getPluginInfos()) {
            PluginLoadStatus status = pluginLoadStatusMap.getOrDefault(pluginInfo.getName(), PluginLoadStatus.INITIAL);
            pluginsWithStatus.add(new PluginInfoWithStatus(pluginInfo, status));
        }
        Collections.sort(pluginsWithStatus, Comparator.comparing(p -> p.getPluginInfo().getName()));
        return pluginsWithStatus;
    }

    /**
     * Get the load status for a specific plugin
     */
    public PluginLoadStatus getPluginLoadStatus(String pluginName) {
        return pluginLoadStatusMap.getOrDefault(pluginName, PluginLoadStatus.INITIAL);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("plugins");
        for (PluginInfoWithStatus pluginInfoWithStatus : getPluginInfosWithStatus()) {
            pluginInfoWithStatus.toXContent(builder, params);
        }
        builder.endArray();
        
        builder.startArray("modules");
        for (PluginInfo moduleInfo : getModuleInfos()) {
            moduleInfo.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }
}
