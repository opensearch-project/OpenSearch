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

import org.apache.lucene.search.Explanation;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.proto.search.SearchHitsProtoDef;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.HighlightFieldTests;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.transport.protobuf.SearchHitProtobuf;

import static org.opensearch.index.get.DocumentFieldTests.randomDocumentField;
import static org.opensearch.search.SearchHitTests.createExplanation;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetToProto;
import static org.opensearch.transport.protobuf.SearchHitProtobuf.searchSortValuesFromProto;
import static org.opensearch.transport.protobuf.SearchHitProtobuf.searchSortValuesToProto;

public class SearchHitProtobufTests extends AbstractWireSerializingTestCase<SearchHitProtobuf> {
    public void testDocumentFieldProtoSerialization() {
        DocumentField orig = randomDocumentField(randomFrom(XContentType.values()), randomBoolean(), fieldName -> false).v1();
        SearchHitsProtoDef.DocumentFieldProto proto = documentFieldToProto(orig);
        DocumentField cpy = documentFieldFromProto(proto);
        assertEquals(orig, cpy);
        assertEquals(orig.hashCode(), cpy.hashCode());
        assertNotSame(orig, cpy);
    }

    public void testHighlightFieldProtoSerialization() {
        HighlightField orig = HighlightFieldTests.createTestItem();
        SearchHitsProtoDef.HighlightFieldProto proto = highlightFieldToProto(orig);
        HighlightField cpy = highlightFieldFromProto(proto);
        assertEquals(orig, cpy);
        assertEquals(orig.hashCode(), cpy.hashCode());
        assertNotSame(orig, cpy);
    }

    public void testSearchSortValuesProtoSerialization() {
        SearchSortValues orig = SearchSortValuesTests.createTestItem(randomFrom(XContentType.values()), true);
        SearchHitsProtoDef.SearchSortValuesProto proto = searchSortValuesToProto(orig);
        SearchSortValues cpy = searchSortValuesFromProto(proto);
        assertEquals(orig, cpy);
        assertEquals(orig.hashCode(), cpy.hashCode());
        assertNotSame(orig, cpy);
    }

    public void testNestedIdentityProtoSerialization() {
        SearchHit.NestedIdentity orig = NestedIdentityTests.createTestItem(randomIntBetween(0, 2));
        SearchHitsProtoDef.NestedIdentityProto proto = SearchHitProtobuf.nestedIdentityToProto(orig);
        SearchHit.NestedIdentity cpy = SearchHitProtobuf.nestedIdentityFromProto(proto);
        assertEquals(orig, cpy);
        assertEquals(orig.hashCode(), cpy.hashCode());
        assertNotSame(orig, cpy);
    }

    public void testSearchShardTargetProtoSerialization() {
        String index = randomAlphaOfLengthBetween(5, 10);
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardTarget orig = new SearchShardTarget(
            randomAlphaOfLengthBetween(5, 10),
            new ShardId(new Index(index, randomAlphaOfLengthBetween(5, 10)), randomInt()),
            clusterAlias,
            OriginalIndices.NONE
        );
        SearchHitsProtoDef.SearchShardTargetProto proto = searchShardTargetToProto(orig);
        SearchShardTarget cpy = searchShardTargetFromProto(proto);
        assertEquals(orig, cpy);
        assertEquals(orig.hashCode(), cpy.hashCode());
        assertNotSame(orig, cpy);
    }

    public void testExplanationProtoSerialization() {
        Explanation orig = createExplanation(randomIntBetween(0, 5));
        SearchHitsProtoDef.ExplanationProto proto = explanationToProto(orig);
        Explanation cpy = explanationFromProto(proto);
        assertEquals(orig, cpy);
        assertEquals(orig.hashCode(), cpy.hashCode());
        assertNotSame(orig, cpy);
    }

    @Override
    protected Writeable.Reader<SearchHitProtobuf> instanceReader() {
        return SearchHitProtobuf::new;
    }

    @Override
    protected SearchHitProtobuf createTestInstance() {
        return new SearchHitProtobuf(SearchHitTests.createTestItem(randomFrom(XContentType.values()), randomBoolean(), randomBoolean()));
    }
}
