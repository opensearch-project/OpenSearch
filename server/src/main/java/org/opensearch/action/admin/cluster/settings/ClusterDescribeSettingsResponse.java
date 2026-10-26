/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Definition metadata for registered settings; contains neither configured values nor validation results.
 * <p>
 * Local-only: the response is built and rendered on the node that received the request and is deliberately not
 * serializable, so it can never be exchanged between nodes of different versions.
 * @opensearch.internal
 */
public class ClusterDescribeSettingsResponse extends ActionResponse implements ToXContentObject {
    private final Map<String, SettingMetadata> settings;

    public ClusterDescribeSettingsResponse(Map<String, SettingMetadata> settings) {
        this.settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    }

    /**
     * Not supported: the response is local-only. This constructor exists only because {@link org.opensearch.action.ActionType}
     * requires a response reader; it fails loudly if the response is ever routed over the transport layer.
     */
    public ClusterDescribeSettingsResponse(StreamInput in) {
        throw new UnsupportedOperationException("ClusterDescribeSettingsResponse is local-only and is not serializable");
    }

    public Map<String, SettingMetadata> settings() {
        return settings;
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("ClusterDescribeSettingsResponse is local-only and is not serializable");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startObject("settings");
        for (var entry : settings.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        return builder.endObject().endObject();
    }

    /**
     * Response-only projection of a single registered setting as reported by this API. This is deliberately separate from
     * {@link org.opensearch.common.settings.Setting}, which holds parser and validator functions. Like the enclosing
     * response it is not serializable.
     * @opensearch.internal
     */
    public static class SettingMetadata implements ToXContentObject {
        private final boolean dynamic;
        @Nullable
        private final String defaultValue;

        /**
         * @param dynamic      whether the setting can be updated on a running cluster
         * @param defaultValue the declared default in its string form, or {@code null} when none should be reported
         */
        private SettingMetadata(boolean dynamic, @Nullable String defaultValue) {
            this.dynamic = dynamic;
            this.defaultValue = defaultValue;
        }

        /** Metadata for a setting whose default is not reported. */
        public static SettingMetadata withoutDefault(boolean dynamic) {
            return new SettingMetadata(dynamic, null);
        }

        /** Metadata for a setting with a declared default. */
        public static SettingMetadata withDefault(boolean dynamic, String defaultValue) {
            return new SettingMetadata(dynamic, Objects.requireNonNull(defaultValue, "defaultValue"));
        }

        public boolean dynamic() {
            return dynamic;
        }

        @Nullable
        public String defaultValue() {
            return defaultValue;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("dynamic", dynamic);
            if (defaultValue != null) {
                builder.field("default", defaultValue);
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SettingMetadata that = (SettingMetadata) o;
            return dynamic == that.dynamic && Objects.equals(defaultValue, that.defaultValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dynamic, defaultValue);
        }

        @Override
        public String toString() {
            return "SettingMetadata{dynamic=" + dynamic + ", default=" + defaultValue + "}";
        }
    }
}
