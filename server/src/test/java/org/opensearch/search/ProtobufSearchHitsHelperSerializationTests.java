/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.HighlightFieldTests;
import org.opensearch.serde.proto.SearchHitsTransportProto;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.index.get.DocumentFieldTests.randomDocumentField;
import static org.opensearch.search.SearchHitTests.createExplanation;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.documentFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.explanationToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.highlightFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.indexFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.indexToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchShardTargetToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchSortValuesFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.searchSortValuesToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.shardIdFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.shardIdToProto;

public class ProtobufSearchHitsHelperSerializationTests extends OpenSearchTestCase {

    public void testProtoExplanationSerDe() {
        Explanation orig = createExplanation(randomIntBetween(0, 5));
        SearchHitsTransportProto.ExplanationProto proto = explanationToProto(orig);
        Explanation cpy = explanationFromProto(proto);
        assertEquals(orig, cpy);
    }

    public void testProtoDocumentFieldSerDe() {
        DocumentField orig = randomDocumentField(randomFrom(XContentType.values()), randomBoolean(), fieldName -> false).v1();
        SearchHitsTransportProto.DocumentFieldProto proto = documentFieldToProto(orig);
        DocumentField cpy = documentFieldFromProto(proto);
        assertEquals(orig, cpy);
    }

    public void testProtoHighlightFieldSerDe() {
        HighlightField orig = HighlightFieldTests.createTestItem();
        SearchHitsTransportProto.HighlightFieldProto proto = highlightFieldToProto(orig);
        HighlightField cpy = highlightFieldFromProto(proto);
        assertEquals(orig, cpy);
    }

    public void testProtoSearchSortValuesSerDe() {
        SearchSortValues orig = SearchSortValuesTests.createTestItem(randomFrom(XContentType.values()), true);
        SearchHitsTransportProto.SearchSortValuesProto proto = searchSortValuesToProto(orig);
        SearchSortValues cpy = searchSortValuesFromProto(proto);
        assertEquals(orig, cpy);
    }

    public void testProtoSearchShardTargetSerDe() {
        String index = randomAlphaOfLengthBetween(5, 10);
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardTarget orig = new SearchShardTarget(
            randomAlphaOfLengthBetween(5, 10),
            new ShardId(new Index(index, randomAlphaOfLengthBetween(5, 10)), randomInt()),
            clusterAlias,
            OriginalIndices.NONE
        );

        SearchHitsTransportProto.SearchShardTargetProto proto = searchShardTargetToProto(orig);
        SearchShardTarget cpy = searchShardTargetFromProto(proto);
        assertEquals(orig, cpy);
    }

    public void testProtoShardIdSerDe() {
        ShardId orig = new ShardId(new Index(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)), randomInt());
        SearchHitsTransportProto.ShardIdProto proto = shardIdToProto(orig);
        ShardId cpy = shardIdFromProto(proto);
        assertEquals(orig, cpy);
    }

    public void testProtoIndexSerDe() {
        Index orig = new Index(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10));
        SearchHitsTransportProto.IndexProto proto = indexToProto(orig);
        Index cpy = indexFromProto(proto);
        assertEquals(orig, cpy);
    }
}
