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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Randomness;
import org.opensearch.common.util.SetBackedScalingCuckooFilter;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.ParsedMultiBucketAggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StringRareTermsTests extends InternalRareTermsTestCase {

    @Override
    protected InternalRareTerms<?, ?> createTestInstance(
        String name,
        Map<String, Object> metadata,
        InternalAggregations aggregations,
        long maxDocCount
    ) {
        BucketOrder order = BucketOrder.count(false);
        DocValueFormat format = DocValueFormat.RAW;
        List<StringRareTerms.Bucket> buckets = new ArrayList<>();
        final int numBuckets = randomNumberOfBuckets();
        for (int i = 0; i < numBuckets; ++i) {
            Set<BytesRef> terms = new HashSet<>();
            BytesRef term = randomValueOtherThanMany(b -> terms.add(b) == false, () -> new BytesRef(randomAlphaOfLength(10)));
            int docCount = randomIntBetween(1, 100);
            buckets.add(new StringRareTerms.Bucket(term, docCount, aggregations, format));
        }
        SetBackedScalingCuckooFilter filter = new SetBackedScalingCuckooFilter(1000, Randomness.get(), 0.01);
        return new StringRareTerms(name, order, metadata, format, buckets, maxDocCount, filter);
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedStringRareTerms.class;
    }

    @Override
    protected InternalRareTerms<?, ?> mutateInstance(InternalRareTerms<?, ?> instance) {
        if (instance instanceof StringRareTerms) {
            StringRareTerms stringRareTerms = (StringRareTerms) instance;
            String name = stringRareTerms.getName();
            BucketOrder order = stringRareTerms.order;
            DocValueFormat format = stringRareTerms.format;
            long maxDocCount = stringRareTerms.maxDocCount;
            Map<String, Object> metadata = stringRareTerms.getMetadata();
            List<StringRareTerms.Bucket> buckets = stringRareTerms.getBuckets();
            switch (between(0, 3)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    maxDocCount = between(1, 5);
                    break;
                case 2:
                    buckets = new ArrayList<>(buckets);
                    buckets.add(
                        new StringRareTerms.Bucket(
                            new BytesRef(randomAlphaOfLengthBetween(1, 10)),
                            randomNonNegativeLong(),
                            InternalAggregations.EMPTY,
                            format
                        )
                    );
                    break;
                case 3:
                    if (metadata == null) {
                        metadata = new HashMap<>(1);
                    } else {
                        metadata = new HashMap<>(instance.getMetadata());
                    }
                    metadata.put(randomAlphaOfLength(15), randomInt());
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            return new StringRareTerms(name, order, metadata, format, buckets, maxDocCount, null);
        } else {
            String name = instance.getName();
            Map<String, Object> metadata = instance.getMetadata();
            switch (between(0, 1)) {
                case 0:
                    name += randomAlphaOfLength(5);
                    break;
                case 1:
                    if (metadata == null) {
                        metadata = new HashMap<>(1);
                    } else {
                        metadata = new HashMap<>(instance.getMetadata());
                    }
                    metadata.put(randomAlphaOfLength(15), randomInt());
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            return new UnmappedRareTerms(name, metadata);
        }
    }
}
