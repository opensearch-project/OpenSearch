/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.opensearch.common.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * A configuration that tells multi-terms aggregations how to retrieve data from index
 * in order to run a specific aggregation.
 *
 * Similar with {@link MultiValuesSourceFieldConfig}, but support value script.
 */
public class MultiTermsValuesSourceConfig implements Writeable, ToXContentObject {
    private final String fieldName;
    private final Object missing;
    private final Script script;
    private final ZoneId timeZone;
    private final ValueType userValueTypeHint;
    private final String format;
    private final IncludeExclude includeExclude;

    private static final String NAME = "field_config";
    public static final ParseField FILTER = new ParseField("filter");

    public interface ParserSupplier {
        ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> apply(
            Boolean scriptable,
            Boolean timezoneAware,
            Boolean valueTypeHinted,
            Boolean formatted
        );
    }

    public static final MultiTermsValuesSourceConfig.ParserSupplier PARSER = (scriptable, timezoneAware, valueTypeHinted, formatted) -> {

        ObjectParser<MultiTermsValuesSourceConfig.Builder, Void> parser = new ObjectParser<>(
            MultiTermsValuesSourceConfig.NAME,
            MultiTermsValuesSourceConfig.Builder::new
        );

        parser.declareString(MultiTermsValuesSourceConfig.Builder::setFieldName, ParseField.CommonFields.FIELD);
        parser.declareField(
            MultiTermsValuesSourceConfig.Builder::setMissing,
            XContentParser::objectText,
            ParseField.CommonFields.MISSING,
            ObjectParser.ValueType.VALUE
        );

        if (scriptable) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setScript,
                (p, context) -> Script.parse(p),
                Script.SCRIPT_PARSE_FIELD,
                ObjectParser.ValueType.OBJECT_OR_STRING
            );
        }

        if (timezoneAware) {
            parser.declareField(MultiTermsValuesSourceConfig.Builder::setTimeZone, p -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return ZoneId.of(p.text());
                } else {
                    return ZoneOffset.ofHours(p.intValue());
                }
            }, ParseField.CommonFields.TIME_ZONE, ObjectParser.ValueType.LONG);
        }

        if (valueTypeHinted) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setUserValueTypeHint,
                p -> ValueType.lenientParse(p.text()),
                ValueType.VALUE_TYPE,
                ObjectParser.ValueType.STRING
            );
        }

        if (formatted) {
            parser.declareField(
                MultiTermsValuesSourceConfig.Builder::setFormat,
                XContentParser::text,
                ParseField.CommonFields.FORMAT,
                ObjectParser.ValueType.STRING
            );
        }

        parser.declareField(
            (b, v) -> b.setIncludeExclude(IncludeExclude.merge(b.getIncludeExclude(), v)),
            IncludeExclude::parseExclude,
            IncludeExclude.EXCLUDE_FIELD,
            ObjectParser.ValueType.STRING_ARRAY
        );

        return parser;
    };

    protected MultiTermsValuesSourceConfig(
        String fieldName,
        Object missing,
        Script script,
        ZoneId timeZone,
        ValueType userValueTypeHint,
        String format,
        IncludeExclude includeExclude
    ) {
        this.fieldName = fieldName;
        this.missing = missing;
        this.script = script;
        this.timeZone = timeZone;
        this.userValueTypeHint = userValueTypeHint;
        this.format = format;
        this.includeExclude = includeExclude;
    }

    public MultiTermsValuesSourceConfig(StreamInput in) throws IOException {
        this.fieldName = in.readOptionalString();
        this.missing = in.readGenericValue();
        this.script = in.readOptionalWriteable(Script::new);
        this.timeZone = in.readOptionalZoneId();
        this.userValueTypeHint = in.readOptionalWriteable(ValueType::readFromStream);
        this.format = in.readOptionalString();
        this.includeExclude = in.readOptionalWriteable(IncludeExclude::new);
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

    public ValueType getUserValueTypeHint() {
        return userValueTypeHint;
    }

    public String getFormat() {
        return format;
    }

    /**
     * Get terms to include and exclude from the aggregation results
     */
    public IncludeExclude getIncludeExclude() {
        return includeExclude;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(fieldName);
        out.writeGenericValue(missing);
        out.writeOptionalWriteable(script);
        out.writeOptionalZoneId(timeZone);
        out.writeOptionalWriteable(userValueTypeHint);
        out.writeOptionalString(format);
        out.writeOptionalWriteable(includeExclude);
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
        if (userValueTypeHint != null) {
            builder.field(AggregationBuilder.CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field(AggregationBuilder.CommonFields.FORMAT.getPreferredName(), format);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MultiTermsValuesSourceConfig that = (MultiTermsValuesSourceConfig) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(missing, that.missing)
            && Objects.equals(script, that.script)
            && Objects.equals(timeZone, that.timeZone)
            && Objects.equals(userValueTypeHint, that.userValueTypeHint)
            && Objects.equals(format, that.format)
            && Objects.equals(includeExclude, that.includeExclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, missing, script, timeZone, userValueTypeHint, format, includeExclude);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class Builder {
        private String fieldName;
        private Object missing = null;
        private Script script = null;
        private ZoneId timeZone = null;
        private ValueType userValueTypeHint = null;
        private String format;
        private IncludeExclude includeExclude = null;

        public IncludeExclude getIncludeExclude() {
            return includeExclude;
        }

        public MultiTermsValuesSourceConfig.Builder setIncludeExclude(IncludeExclude includeExclude) {
            this.includeExclude = includeExclude;
            return this;
        }

        public String getFieldName() {
            return fieldName;
        }

        public MultiTermsValuesSourceConfig.Builder setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Object getMissing() {
            return missing;
        }

        public MultiTermsValuesSourceConfig.Builder setMissing(Object missing) {
            this.missing = missing;
            return this;
        }

        public Script getScript() {
            return script;
        }

        public MultiTermsValuesSourceConfig.Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        public ZoneId getTimeZone() {
            return timeZone;
        }

        public MultiTermsValuesSourceConfig.Builder setTimeZone(ZoneId timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        public ValueType getUserValueTypeHint() {
            return userValueTypeHint;
        }

        public MultiTermsValuesSourceConfig.Builder setUserValueTypeHint(ValueType userValueTypeHint) {
            this.userValueTypeHint = userValueTypeHint;
            return this;
        }

        public String getFormat() {
            return format;
        }

        public MultiTermsValuesSourceConfig.Builder setFormat(String format) {
            this.format = format;
            return this;
        }

        public MultiTermsValuesSourceConfig build() {
            if (Strings.isNullOrEmpty(fieldName) && script == null) {
                throw new IllegalArgumentException(
                    "["
                        + ParseField.CommonFields.FIELD.getPreferredName()
                        + "] and ["
                        + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                        + "] cannot both be null.  "
                        + "Please specify one or the other."
                );
            }
            return new MultiTermsValuesSourceConfig(fieldName, missing, script, timeZone, userValueTypeHint, format, includeExclude);
        }
    }
}
