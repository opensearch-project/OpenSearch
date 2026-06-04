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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation;

import java.io.IOException;

/**
 * A simple value agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedSimpleValue extends ParsedSingleValueNumericMetricsAggregation implements SimpleValue {

    @Override
    public String getType() {
        return InternalSimpleValue.NAME;
    }

    private static final ObjectParser<ParsedSimpleValue, Void> PARSER = new ObjectParser<>(
        ParsedSimpleValue.class.getSimpleName(),
        true,
        ParsedSimpleValue::new
    );

    static {
        declareSingleValueFields(PARSER, Double.NaN);
    }

    public static ParsedSimpleValue fromXContent(XContentParser parser, final String name) {
        ParsedSimpleValue simpleValue = PARSER.apply(parser, null);
        simpleValue.setName(name);
        return simpleValue;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = Double.isNaN(value) == false;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? value : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }
}
