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

package org.opensearch.search;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.lucene.LuceneTests;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.transport.protobuf.SearchHitsProtobuf;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;

public class SearchHitsProtobufTests extends AbstractWireSerializingTestCase<SearchHitsProtobuf> {

    @Override
    protected Writeable.Reader<SearchHitsProtobuf> instanceReader() {
        return SearchHitsProtobuf::new;
    }

    @Override
    protected SearchHitsProtobuf createTestInstance() {
        // This instance is used to test the transport serialization so it's fine
        // to produce shard targets (withShardTarget is true) since they are serialized
        // in this layer.
        return createTestItem(randomFrom(XContentType.values()), true, true);
    }

    public static SearchHitsProtobuf createTestItem(boolean withOptionalInnerHits, boolean withShardTarget) {
        return createTestItem(randomFrom(XContentType.values()), withOptionalInnerHits, withShardTarget);
    }

    public static SearchHitsProtobuf createTestItem(final MediaType mediaType, boolean withOptionalInnerHits, boolean transportSerialization) {
        return createTestItem(mediaType, withOptionalInnerHits, transportSerialization, randomFrom(TotalHits.Relation.values()));
    }

    private static SearchHitsProtobuf createTestItem(
        final MediaType mediaType,
        boolean withOptionalInnerHits,
        boolean transportSerialization,
        TotalHits.Relation totalHitsRelation
    ) {
        int searchHits = randomIntBetween(0, 5);
        SearchHit[] hits = createSearchHitArray(searchHits, mediaType, withOptionalInnerHits, transportSerialization);
        TotalHits totalHits = frequently() ? randomTotalHits(totalHitsRelation) : null;
        float maxScore = frequently() ? randomFloat() : Float.NaN;
        SortField[] sortFields = null;
        String collapseField = null;
        Object[] collapseValues = null;
        if (transportSerialization) {
            sortFields = randomBoolean() ? createSortFields(randomIntBetween(1, 5)) : null;
            collapseField = randomAlphaOfLengthBetween(5, 10);
            collapseValues = randomBoolean() ? createCollapseValues(randomIntBetween(1, 10)) : null;
        }
        return new SearchHitsProtobuf(new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues));
    }

    private static SearchHit[] createSearchHitArray(
        int size,
        final MediaType mediaType,
        boolean withOptionalInnerHits,
        boolean transportSerialization
    ) {
        SearchHit[] hits = new SearchHit[size];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = SearchHitTests.createTestItem(mediaType, withOptionalInnerHits, transportSerialization);
        }
        return hits;
    }

    private static TotalHits randomTotalHits(TotalHits.Relation relation) {
        long totalHits = TestUtil.nextLong(random(), 0, Long.MAX_VALUE);
        return new TotalHits(totalHits, relation);
    }

    private static SortField[] createSortFields(int size) {
        SortField[] sortFields = new SortField[size];
        for (int i = 0; i < sortFields.length; i++) {
            // sort fields are simplified before serialization, we write directly the simplified version
            // otherwise equality comparisons become complicated
            sortFields[i] = LuceneTests.randomSortField().v2();
        }
        return sortFields;
    }

    private static Object[] createCollapseValues(int size) {
        Object[] collapseValues = new Object[size];
        for (int i = 0; i < collapseValues.length; i++) {
            collapseValues[i] = LuceneTests.randomSortValue();
        }
        return collapseValues;
    }

    @Override
    protected SearchHitsProtobuf mutateInstance(SearchHitsProtobuf instance) {
        return new SearchHitsProtobuf(mutate(instance));
    }

    protected SearchHits mutate(SearchHits instance) {
        switch (randomIntBetween(0, 5)) {
            case 0:
                return new SearchHits(
                    createSearchHitArray(instance.getHits().length + 1, randomFrom(XContentType.values()), false, randomBoolean()),
                    instance.getTotalHits(),
                    instance.getMaxScore()
                );
            case 1:
                final TotalHits totalHits;
                if (instance.getTotalHits() == null) {
                    totalHits = randomTotalHits(randomFrom(TotalHits.Relation.values()));
                } else {
                    totalHits = null;
                }
                return new SearchHits(instance.getHits(), totalHits, instance.getMaxScore());
            case 2:
                final float maxScore;
                if (Float.isNaN(instance.getMaxScore())) {
                    maxScore = randomFloat();
                } else {
                    maxScore = Float.NaN;
                }
                return new SearchHits(instance.getHits(), instance.getTotalHits(), maxScore);
            case 3:
                SortField[] sortFields;
                if (instance.getSortFields() == null) {
                    sortFields = createSortFields(randomIntBetween(1, 5));
                } else {
                    sortFields = randomBoolean() ? createSortFields(instance.getSortFields().length + 1) : null;
                }
                return new SearchHits(
                    instance.getHits(),
                    instance.getTotalHits(),
                    instance.getMaxScore(),
                    sortFields,
                    instance.getCollapseField(),
                    instance.getCollapseValues()
                );
            case 4:
                String collapseField;
                if (instance.getCollapseField() == null) {
                    collapseField = randomAlphaOfLengthBetween(5, 10);
                } else {
                    collapseField = randomBoolean() ? instance.getCollapseField() + randomAlphaOfLengthBetween(2, 5) : null;
                }
                return new SearchHits(
                    instance.getHits(),
                    instance.getTotalHits(),
                    instance.getMaxScore(),
                    instance.getSortFields(),
                    collapseField,
                    instance.getCollapseValues()
                );
            case 5:
                Object[] collapseValues;
                if (instance.getCollapseValues() == null) {
                    collapseValues = createCollapseValues(randomIntBetween(1, 5));
                } else {
                    collapseValues = randomBoolean() ? createCollapseValues(instance.getCollapseValues().length + 1) : null;
                }
                return new SearchHits(
                    instance.getHits(),
                    instance.getTotalHits(),
                    instance.getMaxScore(),
                    instance.getSortFields(),
                    instance.getCollapseField(),
                    collapseValues
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
