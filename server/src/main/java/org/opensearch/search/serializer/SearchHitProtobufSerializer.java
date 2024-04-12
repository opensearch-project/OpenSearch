/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.Explanation;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.document.serializer.DocumentFieldProtobufSerializer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHit.NestedIdentity;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchSortValues;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.serializer.HighlightFieldProtobufSerializer;
import org.opensearch.server.proto.FetchSearchResultProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serializer for {@link SearchHit} to/from protobuf.
 */
public class SearchHitProtobufSerializer implements SearchHitSerializer<InputStream> {

    private FetchSearchResultProto.SearchHit searchHitProto;

    @Override
    public SearchHit createSearchHit(InputStream inputStream) throws IOException {
        this.searchHitProto = FetchSearchResultProto.SearchHit.parseFrom(inputStream);
        int docId = -1;
        float score = this.searchHitProto.getScore();
        String id = this.searchHitProto.getId();
        NestedIdentity nestedIdentity;
        if (!this.searchHitProto.hasNestedIdentity() && this.searchHitProto.getNestedIdentity().toByteArray().length > 0) {
            NestedIdentityProtobufSerializer protobufSerializer = new NestedIdentityProtobufSerializer();
            nestedIdentity = protobufSerializer.createNestedIdentity(
                new ByteArrayInputStream(this.searchHitProto.getNestedIdentity().toByteArray())
            );
        } else {
            nestedIdentity = null;
        }
        long version = this.searchHitProto.getVersion();
        long seqNo = this.searchHitProto.getSeqNo();
        long primaryTerm = this.searchHitProto.getPrimaryTerm();
        BytesReference source = BytesReference.fromByteBuffer(ByteBuffer.wrap(this.searchHitProto.getSource().toByteArray()));
        if (source.length() == 0) {
            source = null;
        }
        Map<String, DocumentField> documentFields = new HashMap<>();
        DocumentFieldProtobufSerializer protobufSerializer = new DocumentFieldProtobufSerializer();
        this.searchHitProto.getDocumentFieldsMap().forEach((k, v) -> {
            try {
                documentFields.put(k, protobufSerializer.createDocumentField(new ByteArrayInputStream(v.toByteArray())));
            } catch (IOException e) {
                throw new OpenSearchParseException("failed to parse document field", e);
            }
        });
        Map<String, DocumentField> metaFields = new HashMap<>();
        this.searchHitProto.getMetaFieldsMap().forEach((k, v) -> {
            try {
                metaFields.put(k, protobufSerializer.createDocumentField(new ByteArrayInputStream(v.toByteArray())));
            } catch (IOException e) {
                throw new OpenSearchParseException("failed to parse document field", e);
            }
        });
        Map<String, HighlightField> highlightFields = new HashMap<>();
        HighlightFieldProtobufSerializer highlightFieldProtobufSerializer = new HighlightFieldProtobufSerializer();
        this.searchHitProto.getHighlightFieldsMap().forEach((k, v) -> {
            try {
                highlightFields.put(k, highlightFieldProtobufSerializer.createHighLightField(new ByteArrayInputStream(v.toByteArray())));
            } catch (IOException e) {
                throw new OpenSearchParseException("failed to parse highlight field", e);
            }
        });
        SearchSortValuesProtobufSerializer sortValueProtobufSerializer = new SearchSortValuesProtobufSerializer();
        SearchSortValues sortValues = sortValueProtobufSerializer.createSearchSortValues(
            new ByteArrayInputStream(this.searchHitProto.getSortValues().toByteArray())
        );
        Map<String, Float> matchedQueries = new HashMap<>();
        if (this.searchHitProto.getMatchedQueriesCount() > 0) {
            matchedQueries = new LinkedHashMap<>(this.searchHitProto.getMatchedQueriesCount());
            for (String query : this.searchHitProto.getMatchedQueriesList()) {
                matchedQueries.put(query, Float.NaN);
            }
        }
        if (this.searchHitProto.getMatchedQueriesWithScoresCount() > 0) {
            Map<String, Float> tempMap = this.searchHitProto.getMatchedQueriesWithScoresMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue()));
            matchedQueries = tempMap.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        }
        Explanation explanation = null;
        if (this.searchHitProto.hasExplanation()) {
            explanation = readExplanation(this.searchHitProto.getExplanation().toByteArray());
        }
        SearchShardTarget searchShardTarget = new SearchShardTarget(
            this.searchHitProto.getShard().getNodeId(),
            new ShardId(
                this.searchHitProto.getShard().getShardId().getIndexName(),
                this.searchHitProto.getShard().getShardId().getIndexUUID(),
                this.searchHitProto.getShard().getShardId().getShardId()
            ),
            this.searchHitProto.getShard().getClusterAlias(),
            OriginalIndices.NONE
        );
        Map<String, SearchHits> innerHits;
        if (this.searchHitProto.getInnerHitsCount() > 0) {
            innerHits = new HashMap<>();
            this.searchHitProto.getInnerHitsMap().forEach((k, v) -> {
                try {
                    SearchHitsProtobufSerializer protobufHitsFactory = new SearchHitsProtobufSerializer();
                    innerHits.put(k, protobufHitsFactory.createSearchHits(new ByteArrayInputStream(v.toByteArray())));
                } catch (IOException e) {
                    throw new OpenSearchParseException("failed to parse inner hits", e);
                }
            });
        } else {
            innerHits = null;
        }
        SearchHit searchHit = new SearchHit(docId, id, nestedIdentity, documentFields, metaFields);
        searchHit.score(score);
        searchHit.version(version);
        searchHit.setSeqNo(seqNo);
        searchHit.setPrimaryTerm(primaryTerm);
        searchHit.sourceRef(source);
        searchHit.highlightFields(highlightFields);
        searchHit.sortValues(sortValues);
        searchHit.matchedQueriesWithScores(matchedQueries);
        searchHit.explanation(explanation);
        searchHit.shard(searchShardTarget);
        searchHit.setInnerHits(innerHits);
        return searchHit;
    }

    public static FetchSearchResultProto.SearchHit convertHitToProto(SearchHit hit) {
        FetchSearchResultProto.SearchHit.Builder searchHitBuilder = FetchSearchResultProto.SearchHit.newBuilder();
        if (hit.getIndex() != null) {
            searchHitBuilder.setIndex(hit.getIndex());
        }
        searchHitBuilder.setId(hit.getId());
        searchHitBuilder.setScore(hit.getScore());
        searchHitBuilder.setSeqNo(hit.getSeqNo());
        searchHitBuilder.setPrimaryTerm(hit.getPrimaryTerm());
        searchHitBuilder.setVersion(hit.getVersion());
        searchHitBuilder.setDocId(hit.docId());
        if (hit.getSourceRef() != null) {
            searchHitBuilder.setSource(ByteString.copyFrom(hit.getSourceRef().toBytesRef().bytes));
        }
        for (Map.Entry<String, DocumentField> entry : hit.getFields().entrySet()) {
            searchHitBuilder.putDocumentFields(
                entry.getKey(),
                DocumentFieldProtobufSerializer.convertDocumentFieldToProto(entry.getValue())
            );
        }
        return searchHitBuilder.build();
    }

    public Explanation readExplanation(byte[] in) throws IOException {
        FetchSearchResultProto.SearchHit.Explanation explanationProto = FetchSearchResultProto.SearchHit.Explanation.parseFrom(in);
        boolean match = explanationProto.getMatch();
        String description = explanationProto.getDescription();
        final Explanation[] subExplanations = new Explanation[explanationProto.getSubExplanationsCount()];
        for (int i = 0; i < subExplanations.length; ++i) {
            subExplanations[i] = readExplanation(explanationProto.getSubExplanations(i).toByteArray());
        }
        Number explanationValue = null;
        if (explanationProto.hasValue1()) {
            explanationValue = explanationProto.getValue1();
        } else if (explanationProto.hasValue2()) {
            explanationValue = explanationProto.getValue2();
        } else if (explanationProto.hasValue3()) {
            explanationValue = explanationProto.getValue3();
        }
        if (match) {
            return Explanation.match(explanationValue, description, subExplanations);
        } else {
            return Explanation.noMatch(description, subExplanations);
        }
    }
}
