/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Wrapper for PluginInfo that includes load status information for hot reload functionality
 *
 * @opensearch.internal
 */
public class PluginInfoWithStatus implements Writeable, ToXContentObject {
    
    private final PluginInfo pluginInfo;
    private final PluginLoadStatus loadStatus;
    private final long loadTimestamp;
    
    public PluginInfoWithStatus(PluginInfo pluginInfo, PluginLoadStatus loadStatus) {
        this.pluginInfo = Objects.requireNonNull(pluginInfo);
        this.loadStatus = Objects.requireNonNull(loadStatus);
        this.loadTimestamp = Instant.now().toEpochMilli();
    }
    
    public PluginInfoWithStatus(PluginInfo pluginInfo, PluginLoadStatus loadStatus, long loadTimestamp) {
        this.pluginInfo = Objects.requireNonNull(pluginInfo);
        this.loadStatus = Objects.requireNonNull(loadStatus);
        this.loadTimestamp = loadTimestamp;
    }
    
    public PluginInfoWithStatus(StreamInput in) throws IOException {
        this.pluginInfo = new PluginInfo(in);
        this.loadStatus = PluginLoadStatus.valueOf(in.readString());
        this.loadTimestamp = in.readLong();
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        pluginInfo.writeTo(out);
        out.writeString(loadStatus.name());
        out.writeLong(loadTimestamp);
    }
    
    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }
    
    public PluginLoadStatus getLoadStatus() {
        return loadStatus;
    }
    
    public long getLoadTimestamp() {
        return loadTimestamp;
    }
    
    public String getName() {
        return pluginInfo.getName();
    }
    
    public String getDescription() {
        return pluginInfo.getDescription();
    }
    
    public String getVersion() {
        return pluginInfo.getVersion();
    }
    
    public String getClassname() {
        return pluginInfo.getClassname();
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            // Include all original plugin info fields
            pluginInfo.toXContent(builder, params);
            // Add load status information
            builder.field("load_status", loadStatus.getDisplayName());
            builder.field("load_timestamp", loadTimestamp);
        }
        builder.endObject();
        return builder;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginInfoWithStatus that = (PluginInfoWithStatus) o;
        return Objects.equals(pluginInfo, that.pluginInfo) &&
               loadStatus == that.loadStatus;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(pluginInfo, loadStatus);
    }
    
    @Override
    public String toString() {
        return "PluginInfoWithStatus{" +
               "pluginInfo=" + pluginInfo +
               ", loadStatus=" + loadStatus +
               ", loadTimestamp=" + loadTimestamp +
               '}';
    }
}
