/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
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
     * Similar to {@link InnerHitBuilder#fromXContent(XContentParser)}
     *
     * @param innerHits
     * @throws IOException if there's an error during parsing
     */
    protected static InnerHitBuilder fromProto(List<InnerHits> innerHits) throws IOException {
        InnerHitBuilder innerHitBuilder = new InnerHitBuilder();

        for (InnerHits innerHit : innerHits) {
            if (innerHit.hasName()) {
                innerHitBuilder.setName(innerHit.getName());
            }
            if (innerHit.hasIgnoreUnmapped()) {
                innerHitBuilder.setIgnoreUnmapped(innerHit.getIgnoreUnmapped());
            }
            if (innerHit.hasFrom()) {
                innerHitBuilder.setFrom(innerHit.getFrom());
            }
            if (innerHit.hasSize()) {
                innerHitBuilder.setSize(innerHit.getSize());
            }
            if (innerHit.hasExplain()) {
                innerHitBuilder.setExplain(innerHit.getExplain());
            }
            if (innerHit.hasVersion()) {
                innerHitBuilder.setVersion(innerHit.getVersion());
            }
            if (innerHit.hasSeqNoPrimaryTerm()) {
                innerHitBuilder.setSeqNoAndPrimaryTerm(innerHit.getSeqNoPrimaryTerm());
            }
            if (innerHit.hasTrackScores()) {
                innerHitBuilder.setTrackScores(innerHit.getTrackScores());
            }
            if (innerHit.getStoredFieldsCount() > 0) {
                innerHitBuilder.setStoredFieldNames(innerHit.getStoredFieldsList());
            }
            if (innerHit.getDocvalueFieldsCount() > 0) {
                List<FieldAndFormat> fieldAndFormatList = new ArrayList<>();
                for (org.opensearch.protobufs.FieldAndFormat fieldAndFormat : innerHit.getDocvalueFieldsList()) {
                    fieldAndFormatList.add(FieldAndFormatProtoUtils.fromProto(fieldAndFormat));
                }
                innerHitBuilder.setDocValueFields(fieldAndFormatList);
            }
            if (innerHit.getFieldsCount() > 0) {
                List<FieldAndFormat> fieldAndFormatList = new ArrayList<>();
                for (String fieldName : innerHit.getFieldsList()) {
                    // Convert string field names to FieldAndFormat objects
                    fieldAndFormatList.add(new FieldAndFormat(fieldName, null));
                }
                innerHitBuilder.setFetchFields(fieldAndFormatList);
            }
            if (innerHit.getScriptFieldsCount() > 0) {
                Set<SearchSourceBuilder.ScriptField> scriptFields = new HashSet<>();
                for (Map.Entry<String, ScriptField> entry : innerHit.getScriptFieldsMap().entrySet()) {
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
            if (innerHit.getSortCount() > 0) {
                innerHitBuilder.setSorts(SortBuilderProtoUtils.fromProto(innerHit.getSortList()));
            }
            if (innerHit.hasXSource()) {
                innerHitBuilder.setFetchSourceContext(FetchSourceContextProtoUtils.fromProto(innerHit.getXSource()));
            }
            if (innerHit.hasHighlight()) {
                innerHitBuilder.setHighlightBuilder(HighlightBuilderProtoUtils.fromProto(innerHit.getHighlight()));
            }
            if (innerHit.hasCollapse()) {
                innerHitBuilder.setInnerCollapse(CollapseBuilderProtoUtils.fromProto(innerHit.getCollapse()));
            }
        }
        return innerHitBuilder;
    }

}
