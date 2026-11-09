/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.protobufs.InnerHits;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.transport.grpc.proto.request.common.FetchSourceContextProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.sort.SortBuilderProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for converting SearchSourceBuilder Protocol Buffers to objects
 *
 */
public class InnerHitsBuilderProtoUtils {

    private InnerHitsBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a single protobuf InnerHits to an OpenSearch InnerHitBuilder.
     * Each InnerHits protobuf message represents ONE inner hit definition.
     *
     * @param innerHits the protobuf InnerHits to convert
     * @param registry The registry for query conversion (needed for sorts and highlights with queries)
     * @return the converted OpenSearch InnerHitBuilder
     * @throws IOException if there's an error during parsing
     */
    public static InnerHitBuilder fromProto(InnerHits innerHits, QueryBuilderProtoConverterRegistry registry) throws IOException {
        if (innerHits == null) {
            throw new IllegalArgumentException("InnerHits cannot be null");
        }

        InnerHitBuilder innerHitBuilder = new InnerHitBuilder();

        if (innerHits.hasName()) {
            innerHitBuilder.setName(innerHits.getName());
        }
        if (innerHits.hasIgnoreUnmapped()) {
            innerHitBuilder.setIgnoreUnmapped(innerHits.getIgnoreUnmapped());
        }
        if (innerHits.hasFrom()) {
            innerHitBuilder.setFrom(innerHits.getFrom());
        }
        if (innerHits.hasSize()) {
            innerHitBuilder.setSize(innerHits.getSize());
        }
        if (innerHits.hasExplain()) {
            innerHitBuilder.setExplain(innerHits.getExplain());
        }
        if (innerHits.hasVersion()) {
            innerHitBuilder.setVersion(innerHits.getVersion());
        }
        if (innerHits.hasSeqNoPrimaryTerm()) {
            innerHitBuilder.setSeqNoAndPrimaryTerm(innerHits.getSeqNoPrimaryTerm());
        }
        if (innerHits.hasTrackScores()) {
            innerHitBuilder.setTrackScores(innerHits.getTrackScores());
        }
        if (innerHits.getStoredFieldsCount() > 0) {
            innerHitBuilder.setStoredFieldNames(innerHits.getStoredFieldsList());
        }
        if (innerHits.getDocvalueFieldsCount() > 0) {
            List<FieldAndFormat> docvalueFieldsList = new ArrayList<>();
            for (org.opensearch.protobufs.FieldAndFormat fieldAndFormat : innerHits.getDocvalueFieldsList()) {
                docvalueFieldsList.add(FieldAndFormatProtoUtils.fromProto(fieldAndFormat));
            }
            innerHitBuilder.setDocValueFields(docvalueFieldsList);
        }
        if (innerHits.getFieldsCount() > 0) {
            List<FieldAndFormat> fieldsList = new ArrayList<>();
            for (org.opensearch.protobufs.FieldAndFormat fieldAndFormat : innerHits.getFieldsList()) {
                fieldsList.add(FieldAndFormatProtoUtils.fromProto(fieldAndFormat));
            }
            innerHitBuilder.setFetchFields(fieldsList);
        }
        if (innerHits.getScriptFieldsCount() > 0) {
            Set<SearchSourceBuilder.ScriptField> scriptFields = new HashSet<>();
            for (Map.Entry<String, ScriptField> entry : innerHits.getScriptFieldsMap().entrySet()) {
                String name = entry.getKey();
                ScriptField scriptFieldProto = entry.getValue();
                SearchSourceBuilder.ScriptField scriptField = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
                    name,
                    scriptFieldProto
                );
                scriptFields.add(scriptField);
            }
            innerHitBuilder.setScriptFields(scriptFields);
        }
        if (innerHits.getSortCount() > 0) {
            innerHitBuilder.setSorts(SortBuilderProtoUtils.fromProto(innerHits.getSortList(), registry));
        }
        if (innerHits.hasXSource()) {
            innerHitBuilder.setFetchSourceContext(FetchSourceContextProtoUtils.fromProto(innerHits.getXSource()));
        }
        if (innerHits.hasHighlight()) {
            innerHitBuilder.setHighlightBuilder(HighlightBuilderProtoUtils.fromProto(innerHits.getHighlight(), registry));
        }
        if (innerHits.hasCollapse()) {
            innerHitBuilder.setInnerCollapse(CollapseBuilderProtoUtils.fromProto(innerHits.getCollapse(), registry));
        }

        return innerHitBuilder;
    }

}
