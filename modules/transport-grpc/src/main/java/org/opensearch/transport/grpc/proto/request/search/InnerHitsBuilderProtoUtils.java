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
     * @return the converted OpenSearch InnerHitBuilder
     * @throws IOException if there's an error during parsing
     */
    public static InnerHitBuilder fromProto(InnerHits innerHits) throws IOException {
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
            List<FieldAndFormat> fieldAndFormatList = new ArrayList<>();
            for (org.opensearch.protobufs.FieldAndFormat fieldAndFormat : innerHits.getDocvalueFieldsList()) {
                fieldAndFormatList.add(FieldAndFormatProtoUtils.fromProto(fieldAndFormat));
            }
            innerHitBuilder.setDocValueFields(fieldAndFormatList);
        }
        if (innerHits.getFieldsCount() > 0) {
            List<FieldAndFormat> fieldAndFormatList = new ArrayList<>();
            // TODO: this is not correct, we need to use FieldAndFormatProtoUtils.fromProto() and fix the protobufs in 0.11.0
            for (String fieldName : innerHits.getFieldsList()) {
                fieldAndFormatList.add(new FieldAndFormat(fieldName, null));
            }
            innerHitBuilder.setFetchFields(fieldAndFormatList);
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
            innerHitBuilder.setSorts(SortBuilderProtoUtils.fromProto(innerHits.getSortList()));
        }
        if (innerHits.hasXSource()) {
            innerHitBuilder.setFetchSourceContext(FetchSourceContextProtoUtils.fromProto(innerHits.getXSource()));
        }
        if (innerHits.hasHighlight()) {
            innerHitBuilder.setHighlightBuilder(HighlightBuilderProtoUtils.fromProto(innerHits.getHighlight()));
        }
        if (innerHits.hasCollapse()) {
            innerHitBuilder.setInnerCollapse(CollapseBuilderProtoUtils.fromProto(innerHits.getCollapse()));
        }

        return innerHitBuilder;
    }

    /**
     * Converts a list of protobuf InnerHits to a list of OpenSearch InnerHitBuilder objects.
     * Each InnerHits protobuf message represents ONE inner hit definition.
     *
     * @param innerHitsList the list of protobuf InnerHits to convert
     * @return the list of converted OpenSearch InnerHitBuilder objects
     * @throws IOException if there's an error during parsing
     */
    public static List<InnerHitBuilder> fromProto(List<InnerHits> innerHitsList) throws IOException {
        if (innerHitsList == null) {
            throw new IllegalArgumentException("InnerHits list cannot be null");
        }

        List<InnerHitBuilder> innerHitBuilders = new ArrayList<>();
        for (InnerHits innerHits : innerHitsList) {
            innerHitBuilders.add(fromProto(innerHits));
        }
        return innerHitBuilders;
    }

}
