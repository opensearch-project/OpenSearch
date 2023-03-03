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
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.search.aggregations.bucket.terms.InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME;
import static org.opensearch.search.aggregations.bucket.terms.InternalTerms.SUM_OF_OTHER_DOC_COUNTS;

/**
 * A terms result parsed between nodes
 *
 * @opensearch.internal
 */
public abstract class ParsedTerms extends ParsedMultiBucketAggregation<ParsedTerms.ParsedBucket> implements Terms {

    protected long docCountErrorUpperBound;
    protected long sumOtherDocCount;

    @Override
    public long getDocCountError() {
        return docCountErrorUpperBound;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return sumOtherDocCount;
    }

    @Override
    public List<? extends Terms.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public Terms.Bucket getBucketByKey(String term) {
        for (Terms.Bucket bucket : getBuckets()) {
            if (bucket.getKeyAsString().equals(term)) {
                return bucket;
            }
        }
        return null;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), getSumOfOtherDocCounts());
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Terms.Bucket bucket : getBuckets()) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    static void declareParsedTermsFields(
        final ObjectParser<? extends ParsedTerms, Void> objectParser,
        final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser
    ) {
        declareMultiBucketAggregationFields(objectParser, bucketParser::apply, bucketParser::apply);
        objectParser.declareLong(
            (parsedTerms, value) -> parsedTerms.docCountErrorUpperBound = value,
            DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME
        );
        objectParser.declareLong((parsedTerms, value) -> parsedTerms.sumOtherDocCount = value, SUM_OF_OTHER_DOC_COUNTS);
    }

    /**
     * Base parsed bucket
     *
     * @opensearch.internal
     */
    public abstract static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Terms.Bucket {

        boolean showDocCountError = false;
        protected long docCountError;

        @Override
        public long getDocCountError() {
            return docCountError;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        static <B extends ParsedBucket> B parseTermsBucketXContent(
            final XContentParser parser,
            final Supplier<B> bucketSupplier,
            final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer
        ) throws IOException {

            final B bucket = bucketSupplier.get();
            final List<Aggregation> aggregations = new ArrayList<>();

            XContentParser.Token token;
            String currentFieldName = parser.currentName();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                // field value could be list, e.g. multi_terms aggregation.
                if ((token.isValue() || token == XContentParser.Token.START_ARRAY)
                    && CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                    keyConsumer.accept(parser, bucket);
                }
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    } else if (DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName().equals(currentFieldName)) {
                        bucket.docCountError = parser.longValue();
                        bucket.showDocCountError = true;
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
