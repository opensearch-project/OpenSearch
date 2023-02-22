/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.support;

import org.opensearch.core.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.TriFunction;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.script.Script;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Objects;

/**
 * Base field configuration class for multi values
 *
 * @opensearch.internal
 */
public class MultiValuesSourceFieldConfig extends BaseMultiValuesSourceFieldConfig {
    private final QueryBuilder filter;

    private static final String NAME = "field_config";
    public static final ParseField FILTER = new ParseField("filter");

    public static final TriFunction<Boolean, Boolean, Boolean, ObjectParser<Builder, Void>> PARSER = (
        scriptable,
        timezoneAware,
        filtered) -> {

        ObjectParser<MultiValuesSourceFieldConfig.Builder, Void> parser = new ObjectParser<>(
            MultiValuesSourceFieldConfig.NAME,
            MultiValuesSourceFieldConfig.Builder::new
        );

        BaseMultiValuesSourceFieldConfig.PARSER.apply(parser, scriptable, timezoneAware);

        if (filtered) {
            parser.declareField(
                MultiValuesSourceFieldConfig.Builder::setFilter,
                (p, context) -> AbstractQueryBuilder.parseInnerQueryBuilder(p),
                FILTER,
                ObjectParser.ValueType.OBJECT
            );
        }
        return parser;
    };

    protected MultiValuesSourceFieldConfig(String fieldName, Object missing, Script script, ZoneId timeZone, QueryBuilder filter) {
        super(fieldName, missing, script, timeZone);
        this.filter = filter;
    }

    public MultiValuesSourceFieldConfig(StreamInput in) throws IOException {
        super(in);
        this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(filter);
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (filter != null) {
            builder.field(FILTER.getPreferredName());
            filter.toXContent(builder, params);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;

        MultiValuesSourceFieldConfig that = (MultiValuesSourceFieldConfig) o;
        return Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filter);
    }

    /**
     * Builder for the field config
     *
     * @opensearch.internal
     */
    public static class Builder extends BaseMultiValuesSourceFieldConfig.Builder<BaseMultiValuesSourceFieldConfig, Builder> {
        private QueryBuilder filter = null;

        public Builder setFilter(QueryBuilder filter) {
            this.filter = filter;
            return this;
        }

        public MultiValuesSourceFieldConfig build() {
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

            if (Strings.isNullOrEmpty(fieldName) == false && script != null) {
                throw new IllegalArgumentException(
                    "["
                        + ParseField.CommonFields.FIELD.getPreferredName()
                        + "] and ["
                        + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                        + "] cannot both be configured.  "
                        + "Please specify one or the other."
                );
            }

            return new MultiValuesSourceFieldConfig(fieldName, missing, script, timeZone, filter);
        }
    }
}
