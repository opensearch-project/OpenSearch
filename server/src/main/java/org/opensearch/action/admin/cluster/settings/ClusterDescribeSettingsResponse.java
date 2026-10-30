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
        @Nullable
        private final SettingValidation validation;
        @Nullable
        private final String description;

        /**
         * @param dynamic      whether the setting can be updated on a running cluster
         * @param defaultValue the declared default in its string form, or {@code null} when none should be reported
         * @param validation   the declared value type and bounds, or {@code null} when neither is known
         * @param description  the declared description, or {@code null} when none has been written
         */
        private SettingMetadata(
            boolean dynamic,
            @Nullable String defaultValue,
            @Nullable SettingValidation validation,
            @Nullable String description
        ) {
            this.dynamic = dynamic;
            this.defaultValue = defaultValue;
            this.validation = validation;
            this.description = description;
        }

        /** Metadata for a setting whose default is not reported. */
        public static SettingMetadata withoutDefault(boolean dynamic) {
            return new SettingMetadata(dynamic, null, null, null);
        }

        /** Metadata for a setting with a declared default. */
        public static SettingMetadata withDefault(boolean dynamic, String defaultValue) {
            return new SettingMetadata(dynamic, Objects.requireNonNull(defaultValue, "defaultValue"), null, null);
        }

        /** Returns a copy of this metadata carrying the given validation metadata; {@code null} leaves {@code validation} unset. */
        public SettingMetadata withValidation(@Nullable SettingValidation validation) {
            return new SettingMetadata(dynamic, defaultValue, validation, description);
        }

        /** Returns a copy of this metadata carrying the given description; {@code null} leaves it unset. */
        public SettingMetadata withDescription(@Nullable String description) {
            return new SettingMetadata(dynamic, defaultValue, validation, description);
        }

        public boolean dynamic() {
            return dynamic;
        }

        @Nullable
        public String defaultValue() {
            return defaultValue;
        }

        @Nullable
        public SettingValidation validation() {
            return validation;
        }

        @Nullable
        public String description() {
            return description;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("dynamic", dynamic);
            if (defaultValue != null) {
                builder.field("default", defaultValue);
            }
            if (validation != null) {
                builder.field("validation");
                validation.toXContent(builder, params);
            }
            if (description != null) {
                builder.field("description", description);
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SettingMetadata that = (SettingMetadata) o;
            return dynamic == that.dynamic
                && Objects.equals(defaultValue, that.defaultValue)
                && Objects.equals(validation, that.validation)
                && Objects.equals(description, that.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dynamic, defaultValue, validation, description);
        }

        @Override
        public String toString() {
            return "SettingMetadata{dynamic="
                + dynamic
                + ", default="
                + defaultValue
                + ", validation="
                + validation
                + ", description="
                + description
                + "}";
        }
    }

    /**
     * Declared constraints of a single registered setting: the value type, and the inclusive minimum and maximum when the
     * setting declares them. Exists only to marshal this response; it is derived from, not stored on,
     * {@link org.opensearch.common.settings.Setting}. Like the enclosing response it is not serializable.
     * @opensearch.internal
     */
    public static final class SettingValidation implements ToXContentObject {
        private final String type;
        @Nullable
        private final String min;
        @Nullable
        private final String max;

        private SettingValidation(String type, @Nullable String min, @Nullable String max) {
            this.type = Objects.requireNonNull(type, "type");
            this.min = min;
            this.max = max;
        }

        /**
         * Constraints for a setting whose value type is known.
         *
         * @param type the value type, for example {@code integer}, {@code boolean} or {@code byte_size}
         * @param min  the inclusive lower bound in its string form, or {@code null} when none is declared
         * @param max  the inclusive upper bound in its string form, or {@code null} when none is declared
         */
        public static SettingValidation of(String type, @Nullable String min, @Nullable String max) {
            return new SettingValidation(type, min, max);
        }

        /** Constraints for a setting whose value type is known but which declares no bounds. */
        public static SettingValidation ofType(String type) {
            return new SettingValidation(type, null, null);
        }

        public String type() {
            return type;
        }

        @Nullable
        public String min() {
            return min;
        }

        @Nullable
        public String max() {
            return max;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject().field("type", type);
            if (min != null) {
                builder.field("min", min);
            }
            if (max != null) {
                builder.field("max", max);
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SettingValidation that = (SettingValidation) o;
            return type.equals(that.type) && Objects.equals(min, that.min) && Objects.equals(max, that.max);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, min, max);
        }

        @Override
        public String toString() {
            return "SettingValidation{type=" + type + ", min=" + min + ", max=" + max + "}";
        }
    }
}
