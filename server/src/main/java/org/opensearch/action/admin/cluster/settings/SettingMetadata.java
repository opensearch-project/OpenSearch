/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/** Definition metadata for a single registered setting; contains neither configured values nor validation results.
 * @opensearch.internal
 */
public class SettingMetadata implements Writeable, ToXContentObject {
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

    public SettingMetadata(StreamInput in) throws IOException {
        this(in.readBoolean(), in.readOptionalString());
    }

    public boolean dynamic() {
        return dynamic;
    }

    @Nullable
    public String defaultValue() {
        return defaultValue;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(dynamic);
        out.writeOptionalString(defaultValue);
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
