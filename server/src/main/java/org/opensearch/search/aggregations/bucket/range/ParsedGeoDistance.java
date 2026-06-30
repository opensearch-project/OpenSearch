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

package org.opensearch.search.aggregations.bucket.range;

import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * A geo distance range agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedGeoDistance extends ParsedRange {

    @Override
    public String getType() {
        return GeoDistanceAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedGeoDistance, Void> PARSER = new ObjectParser<>(
        ParsedGeoDistance.class.getSimpleName(),
        true,
        ParsedGeoDistance::new
    );
    static {
        declareParsedRangeFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedGeoDistance fromXContent(XContentParser parser, String name) throws IOException {
        ParsedGeoDistance aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    /**
     * Parsed bucket for a geo distance
     *
     * @opensearch.internal
     */
    public static class ParsedBucket extends ParsedRange.ParsedBucket {

        static ParsedBucket fromXContent(final XContentParser parser, final boolean keyed) throws IOException {
            return parseRangeBucketXContent(parser, ParsedBucket::new, keyed);
        }
    }
}
