/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.support;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * A configuration that used by multi_terms aggregations.
 *
 * @opensearch.internal
 */
public class MultiTermsValuesSourceConfig extends BaseMultiValuesSourceFieldConfig {
    private final ValueType userValueTypeHint;
    private final String format;
    private final IncludeExclude includeExclude;

    private static final String NAME = "field_config";
    public static final ParseField FILTER = new ParseField("filter");

    /**
     * Parser supplier function
     *
     * @opensearch.internal
     */
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

        BaseMultiValuesSourceFieldConfig.PARSER.apply(parser, scriptable, timezoneAware);

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
        super(fieldName, missing, script, timeZone);
        this.userValueTypeHint = userValueTypeHint;
        this.format = format;
        this.includeExclude = includeExclude;
    }

    public MultiTermsValuesSourceConfig(StreamInput in) throws IOException {
        super(in);
        this.userValueTypeHint = in.readOptionalWriteable(ValueType::readFromStream);
        this.format = in.readOptionalString();
        this.includeExclude = in.readOptionalWriteable(IncludeExclude::new);
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
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(userValueTypeHint);
        out.writeOptionalString(format);
        out.writeOptionalWriteable(includeExclude);
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (userValueTypeHint != null) {
            builder.field(AggregationBuilder.CommonFields.VALUE_TYPE.getPreferredName(), userValueTypeHint.getPreferredName());
        }
        if (format != null) {
            builder.field(AggregationBuilder.CommonFields.FORMAT.getPreferredName(), format);
        }
        if (includeExclude != null) {
            includeExclude.toXContent(builder, params);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;

        MultiTermsValuesSourceConfig that = (MultiTermsValuesSourceConfig) o;
        return Objects.equals(userValueTypeHint, that.userValueTypeHint)
            && Objects.equals(format, that.format)
            && Objects.equals(includeExclude, that.includeExclude);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userValueTypeHint, format, includeExclude);
    }

    /**
     * Builder for the multi terms values source configuration
     *
     * @opensearch.internal
     */
    public static class Builder extends BaseMultiValuesSourceFieldConfig.Builder<MultiTermsValuesSourceConfig, Builder> {
        private ValueType userValueTypeHint = null;
        private String format;
        private IncludeExclude includeExclude = null;

        public IncludeExclude getIncludeExclude() {
            return includeExclude;
        }

        public Builder setIncludeExclude(IncludeExclude includeExclude) {
            this.includeExclude = includeExclude;
            return this;
        }

        public ValueType getUserValueTypeHint() {
            return userValueTypeHint;
        }

        public Builder setUserValueTypeHint(ValueType userValueTypeHint) {
            this.userValueTypeHint = userValueTypeHint;
            return this;
        }

        public String getFormat() {
            return format;
        }

        public Builder setFormat(String format) {
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
