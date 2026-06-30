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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A significant terms result parsed between nodes
 *
 * @opensearch.internal
 */
public abstract class ParsedSignificantTerms extends ParsedMultiBucketAggregation<ParsedSignificantTerms.ParsedBucket>
    implements
        SignificantTerms {

    private Map<String, ParsedBucket> bucketMap;
    protected long subsetSize;
    protected long supersetSize;

    protected long getSubsetSize() {
        return subsetSize;
    }

    protected long getSupersetSize() {
        return supersetSize;
    }

    @Override
    public List<? extends SignificantTerms.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public SignificantTerms.Bucket getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(SignificantTerms.Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
    }

    @Override
    public Iterator<SignificantTerms.Bucket> iterator() {
        return buckets.stream().map(bucket -> (SignificantTerms.Bucket) bucket).collect(Collectors.toList()).iterator();
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), subsetSize);
        builder.field(InternalMappedSignificantTerms.BG_COUNT, supersetSize);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (SignificantTerms.Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    static <T extends ParsedSignificantTerms> T parseSignificantTermsXContent(
        final CheckedSupplier<T, IOException> aggregationSupplier,
        final String name
    ) throws IOException {
        T aggregation = aggregationSupplier.get();
        aggregation.setName(name);
        for (ParsedBucket bucket : aggregation.buckets) {
            bucket.subsetSize = aggregation.subsetSize;
            bucket.supersetSize = aggregation.supersetSize;
        }
        return aggregation;
    }

    static void declareParsedSignificantTermsFields(
        final ObjectParser<? extends ParsedSignificantTerms, Void> objectParser,
        final CheckedFunction<XContentParser, ParsedSignificantTerms.ParsedBucket, IOException> bucketParser
    ) {
        declareMultiBucketAggregationFields(objectParser, bucketParser::apply, bucketParser::apply);
        objectParser.declareLong((parsedTerms, value) -> parsedTerms.subsetSize = value, CommonFields.DOC_COUNT);
        objectParser.declareLong(
            (parsedTerms, value) -> parsedTerms.supersetSize = value,
            new ParseField(InternalMappedSignificantTerms.BG_COUNT)
        );
    }

    /**
     * Parsed bucket for significant values
     *
     * @opensearch.internal
     */
    public abstract static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements SignificantTerms.Bucket {

        protected long subsetDf;
        protected long subsetSize;
        protected long supersetDf;
        protected long supersetSize;
        protected double score;

        @Override
        public long getDocCount() {
            return getSubsetDf();
        }

        @Override
        public long getSubsetDf() {
            return subsetDf;
        }

        @Override
        public long getSupersetDf() {
            return supersetDf;
        }

        @Override
        public double getSignificanceScore() {
            return score;
        }

        @Override
        public long getSupersetSize() {
            return supersetSize;
        }

        @Override
        public long getSubsetSize() {
            return subsetSize;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            builder.field(InternalSignificantTerms.SCORE, getSignificanceScore());
            builder.field(InternalSignificantTerms.BG_COUNT, getSupersetDf());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;

        static <B extends ParsedBucket> B parseSignificantTermsBucketXContent(
            final XContentParser parser,
            final B bucket,
            final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer
        ) throws IOException {

            final List<Aggregation> aggregations = new ArrayList<>();
            XContentParser.Token token;
            String currentFieldName = parser.currentName();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        long value = parser.longValue();
                        bucket.subsetDf = value;
                        bucket.setDocCount(value);
                    } else if (InternalSignificantTerms.SCORE.equals(currentFieldName)) {
                        bucket.score = parser.doubleValue();
                    } else if (InternalSignificantTerms.BG_COUNT.equals(currentFieldName)) {
                        bucket.supersetDf = parser.longValue();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    XContentParserUtils.parseTypedKeysObject(
                        parser,
                        Aggregation.TYPED_KEYS_DELIMITER,
                        Aggregation.class,
                        aggregations::add
                    );
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }
    }
}
