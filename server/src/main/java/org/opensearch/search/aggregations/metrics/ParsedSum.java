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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A sum agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedSum extends ParsedSingleValueNumericMetricsAggregation implements Sum {

    private boolean hasValue = true;

    @Override
    public double getValue() {
        return value();
    }

    @Override
    public String getType() {
        return SumAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    private static final ObjectParser<ParsedSum, Void> PARSER = new ObjectParser<>(ParsedSum.class.getSimpleName(), true, ParsedSum::new);

    static {
        // Unlike sibling Parsed* classes, we cannot use declareSingleValueFields() here.
        // That helper uses a sentinel double (e.g. POSITIVE_INFINITY) to represent null,
        // but POSITIVE_INFINITY is a legitimate sum result (e.g. summing Double.MAX_VALUE
        // values). We need a custom parser that tracks null via a boolean flag instead.
        declareAggregationFields(PARSER);
        PARSER.declareField((agg, val) -> {
            if (val == null) {
                agg.hasValue = false;
                agg.value = Double.POSITIVE_INFINITY;
            } else {
                agg.hasValue = true;
                agg.value = val;
            }
        }, (parser, context) -> {
            if (parser.currentToken() == XContentParser.Token.VALUE_NUMBER || parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return parser.doubleValue();
            }
            return null;
        }, CommonFields.VALUE, ObjectParser.ValueType.DOUBLE_OR_NULL);
        PARSER.declareString(ParsedSingleValueNumericMetricsAggregation::setValueAsString, CommonFields.VALUE_AS_STRING);
    }

    public static ParsedSum fromXContent(XContentParser parser, final String name) {
        ParsedSum sum = PARSER.apply(parser, null);
        sum.setName(name);
        return sum;
    }
}
