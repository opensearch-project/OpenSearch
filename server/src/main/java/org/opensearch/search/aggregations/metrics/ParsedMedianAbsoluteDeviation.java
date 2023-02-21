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
 * A median absolute deviation agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedMedianAbsoluteDeviation extends ParsedSingleValueNumericMetricsAggregation implements MedianAbsoluteDeviation {

    private static final ObjectParser<ParsedMedianAbsoluteDeviation, Void> PARSER = new ObjectParser<>(
        ParsedMedianAbsoluteDeviation.class.getSimpleName(),
        true,
        ParsedMedianAbsoluteDeviation::new
    );

    static {
        declareSingleValueFields(PARSER, Double.NaN);
    }

    public static ParsedMedianAbsoluteDeviation fromXContent(XContentParser parser, String name) {
        ParsedMedianAbsoluteDeviation parsedMedianAbsoluteDeviation = PARSER.apply(parser, null);
        parsedMedianAbsoluteDeviation.setName(name);
        return parsedMedianAbsoluteDeviation;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean hasValue = Double.isFinite(getMedianAbsoluteDeviation());
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? getMedianAbsoluteDeviation() : null);
        if (hasValue && valueAsString != null) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), valueAsString);
        }
        return builder;
    }

    @Override
    public double getMedianAbsoluteDeviation() {
        return value();
    }

    @Override
    public String getType() {
        return MedianAbsoluteDeviationAggregationBuilder.NAME;
    }
}
