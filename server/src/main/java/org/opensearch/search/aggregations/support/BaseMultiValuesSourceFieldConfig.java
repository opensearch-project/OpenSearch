/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.opensearch.core.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.TriConsumer;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.script.Script;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * A configuration that tells aggregation how to retrieve data from index
 * in order to run a specific aggregation.
 *
 * @opensearch.internal
 */
public abstract class BaseMultiValuesSourceFieldConfig implements Writeable, ToXContentObject {
    private final String fieldName;
    private final Object missing;
    private final Script script;
    private final ZoneId timeZone;

    static TriConsumer<
        ObjectParser<? extends Builder<? extends BaseMultiValuesSourceFieldConfig, ? extends Builder<?, ?>>, Void>,
        Boolean,
        Boolean> PARSER = (parser, scriptable, timezoneAware) -> {
            parser.declareString(Builder::setFieldName, ParseField.CommonFields.FIELD);
            parser.declareField(
                Builder::setMissing,
                XContentParser::objectText,
                ParseField.CommonFields.MISSING,
                ObjectParser.ValueType.VALUE
            );

            if (scriptable) {
                parser.declareField(
                    Builder::setScript,
                    (p, context) -> Script.parse(p),
                    Script.SCRIPT_PARSE_FIELD,
                    ObjectParser.ValueType.OBJECT_OR_STRING
                );
            }

            if (timezoneAware) {
                parser.declareField(Builder::setTimeZone, p -> {
                    if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                        return ZoneId.of(p.text());
                    } else {
                        return ZoneOffset.ofHours(p.intValue());
                    }
                }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
            }
        };

    public BaseMultiValuesSourceFieldConfig(String fieldName, Object missing, Script script, ZoneId timeZone) {
        this.fieldName = fieldName;
        this.missing = missing;
        this.script = script;
        this.timeZone = timeZone;
    }

    public BaseMultiValuesSourceFieldConfig(StreamInput in) throws IOException {
        this.fieldName = in.readOptionalString();
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        this.timeZone = in.readOptionalZoneId();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(fieldName);
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        out.writeOptionalZoneId(timeZone);
        doWriteTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (missing != null) {
            builder.field(ParseField.CommonFields.MISSING.getPreferredName(), missing);
        }
        if (script != null) {
            builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        }
        if (fieldName != null) {
            builder.field(ParseField.CommonFields.FIELD.getPreferredName(), fieldName);
        }
        if (timeZone != null) {
            builder.field(ParseField.CommonFields.TIME_ZONE.getPreferredName(), timeZone.getId());
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public Object getMissing() {
        return missing;
    }

    public Script getScript() {
        return script;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseMultiValuesSourceFieldConfig that = (BaseMultiValuesSourceFieldConfig) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(missing, that.missing)
            && Objects.equals(script, that.script)
            && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, missing, script, timeZone);
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this);
    }

    abstract void doXContentBody(XContentBuilder builder, Params params) throws IOException;

    abstract void doWriteTo(StreamOutput out) throws IOException;

    /**
     * Base builder for the multi values source field configuration
     *
     * @opensearch.internal
     */
    public abstract static class Builder<C extends BaseMultiValuesSourceFieldConfig, B extends Builder<C, B>> {
        String fieldName;
        Object missing = null;
        Script script = null;
        ZoneId timeZone = null;

        public String getFieldName() {
            return fieldName;
        }

        public B setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return (B) this;
        }

        public Object getMissing() {
            return missing;
        }

        public B setMissing(Object missing) {
            this.missing = missing;
            return (B) this;
        }

        public Script getScript() {
            return script;
        }

        public B setScript(Script script) {
            this.script = script;
            return (B) this;
        }

        public ZoneId getTimeZone() {
            return timeZone;
        }

        public B setTimeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return (B) this;
        }

        abstract public C build();
    }
}
