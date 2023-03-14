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
import org.opensearch.search.aggregations.ParsedAggregation;

import java.io.IOException;

/**
 * A value count agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedValueCount extends ParsedAggregation implements ValueCount {

    private long valueCount;

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return valueCount;
    }

    @Override
    public String getValueAsString() {
        // InternalValueCount doesn't print "value_as_string", but you can get a formatted value using
        // getValueAsString() using the raw formatter and converting the value to double
        return Double.toString(valueCount);
    }

    @Override
    public String getType() {
        return ValueCountAggregationBuilder.NAME;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), valueCount);
        return builder;
    }

    private static final ObjectParser<ParsedValueCount, Void> PARSER = new ObjectParser<>(
        ParsedValueCount.class.getSimpleName(),
        true,
        ParsedValueCount::new
    );

    static {
        declareAggregationFields(PARSER);
        PARSER.declareLong((agg, value) -> agg.valueCount = value, CommonFields.VALUE);
    }

    public static ParsedValueCount fromXContent(XContentParser parser, final String name) {
        ParsedValueCount sum = PARSER.apply(parser, null);
        sum.setName(name);
        return sum;
    }
}
