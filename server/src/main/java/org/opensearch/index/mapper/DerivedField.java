/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * DerivedField representation: expects a name, type and script.
 */
@PublicApi(since = "2.14.0")
public class DerivedField implements Writeable, ToXContentFragment {
    private final String name;
    private final String type;
    private final Script script;
    private String sourceIndexedField;
    private Map<String, Object> properties;
    private Boolean ignoreMalformed;
    private String format;

    public DerivedField(String name, String type, Script script) {
        this.name = name;
        this.type = type;
        this.script = script;
    }

    public DerivedField(StreamInput in) throws IOException {
        name = in.readString();
        type = in.readString();
        script = new Script(in);
        if (in.getVersion().onOrAfter(Version.V_2_15_0)) {
            if (in.readBoolean()) {
                properties = in.readMap();
            }
            sourceIndexedField = in.readOptionalString();
            format = in.readOptionalString();
            ignoreMalformed = in.readOptionalBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        script.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_15_0)) {
            if (properties == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeMap(properties);
            }
            out.writeOptionalString(sourceIndexedField);
            out.writeOptionalString(format);
            out.writeOptionalBoolean(ignoreMalformed);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", type);
        builder.field("script", script);
        if (properties != null) {
            builder.field("properties", properties);
        }
        if (sourceIndexedField != null) {
            builder.field("source_indexed_field", sourceIndexedField);
        }
        if (format != null) {
            builder.field("format", format);
        }
        if (ignoreMalformed != null) {
            builder.field("ignore_malformed", ignoreMalformed);
        }
        builder.endObject();
        return builder;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Script getScript() {
        return script;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getSourceIndexedField() {
        return sourceIndexedField;
    }

    public String getFormat() {
        return format;
    }

    public boolean getIgnoreMalformed() {
        return Boolean.TRUE.equals(ignoreMalformed);
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public void setSourceIndexedField(String sourceIndexedField) {
        this.sourceIndexedField = sourceIndexedField;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setIgnoreMalformed(boolean ignoreMalformed) {
        this.ignoreMalformed = ignoreMalformed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, script, sourceIndexedField, properties, ignoreMalformed, format);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DerivedField other = (DerivedField) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(type, other.type)
            && Objects.equals(script, other.script)
            && Objects.equals(sourceIndexedField, other.sourceIndexedField)
            && Objects.equals(properties, other.properties)
            && Objects.equals(ignoreMalformed, other.ignoreMalformed)
            && Objects.equals(format, other.format);
    }
}
