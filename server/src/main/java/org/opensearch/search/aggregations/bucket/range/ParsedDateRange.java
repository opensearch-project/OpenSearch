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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * A date range agg result parsed between nodes
 *
 * @opensearch.internal
 */
public class ParsedDateRange extends ParsedRange {

    @Override
    public String getType() {
        return DateRangeAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedDateRange, Void> PARSER = new ObjectParser<>(
        ParsedDateRange.class.getSimpleName(),
        true,
        ParsedDateRange::new
    );
    static {
        declareParsedRangeFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedDateRange fromXContent(XContentParser parser, String name) throws IOException {
        ParsedDateRange aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    /**
     * Parsed bucket for a date range
     *
     * @opensearch.internal
     */
    public static class ParsedBucket extends ParsedRange.ParsedBucket {

        @Override
        public Object getFrom() {
            return doubleAsDateTime(from);
        }

        @Override
        public Object getTo() {
            return doubleAsDateTime(to);
        }

        private static ZonedDateTime doubleAsDateTime(Double d) {
            if (d == null || Double.isInfinite(d)) {
                return null;
            }
            return Instant.ofEpochMilli(d.longValue()).atZone(ZoneOffset.UTC);
        }

        static ParsedBucket fromXContent(final XContentParser parser, final boolean keyed) throws IOException {
            return parseRangeBucketXContent(parser, ParsedBucket::new, keyed);
        }
    }
}
