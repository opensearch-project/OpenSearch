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

package org.opensearch.search.aggregations.bucket.terms.heuristic;

import org.opensearch.OpenSearchParseException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardException;

import java.io.IOException;

/**
 * Percentage score significance heuristic for significant terms agg
 *
 * @opensearch.internal
 */
public class PercentageScore extends SignificanceHeuristic {
    public static final String NAME = "percentage";
    public static final ObjectParser<PercentageScore, Void> PARSER = new ObjectParser<>(NAME, PercentageScore::new);

    public PercentageScore() {}

    public PercentageScore(StreamInput in) {
        // Nothing to read.
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME).endObject();
        return builder;
    }

    public static SignificanceHeuristic parse(XContentParser parser) throws IOException, QueryShardException {
        // move to the closing bracket
        if (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
            throw new OpenSearchParseException(
                "failed to parse [percentage] significance heuristic. expected an empty object, " + "but got [{}] instead",
                parser.currentToken()
            );
        }
        return new PercentageScore();
    }

    /**
     * Indicates the significance of a term in a sample by determining what percentage
     * of all occurrences of a term are found in the sample.
     */
    @Override
    public double getScore(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
        checkFrequencyValidity(subsetFreq, subsetSize, supersetFreq, supersetSize, "PercentageScore");
        if (supersetFreq == 0) {
            // avoid a divide by zero issue
            return 0;
        }
        return (double) subsetFreq / (double) supersetFreq;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    /**
     * Builder for a Percentage Score heuristic
     *
     * @opensearch.internal
     */
    public static class PercentageScoreBuilder implements SignificanceHeuristicBuilder {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME).endObject();
            return builder;
        }
    }
}
