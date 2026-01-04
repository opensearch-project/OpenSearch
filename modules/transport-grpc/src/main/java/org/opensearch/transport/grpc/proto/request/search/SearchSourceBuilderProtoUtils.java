/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.DerivedField;
import org.opensearch.protobufs.FieldAndFormat;
import org.opensearch.protobufs.Rescore;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.protobufs.SearchRequestBody;
import org.opensearch.protobufs.TrackHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.transport.grpc.proto.request.common.FetchSourceContextProtoUtils;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.sort.SortBuilderProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.search.builder.SearchSourceBuilder.TIMEOUT_FIELD;
import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;

/**
 * Utility class for converting SearchSourceBuilder Protocol Buffers to objects
 *
 */
public class SearchSourceBuilderProtoUtils {

    private SearchSourceBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Parses a protobuf SearchRequestBody into a SearchSourceBuilder using an instance-based query utils.
     * This method is equivalent to {@link SearchSourceBuilder#parseXContent(XContentParser, boolean)}
     *
     * @param searchSourceBuilder The SearchSourceBuilder to populate
     * @param protoRequest The Protocol Buffer SearchRequest to parse
     * @param queryUtils The query utils instance to use for parsing queries
     * @throws IOException if there's an error during parsing
     */
    public static void parseProto(
        SearchSourceBuilder searchSourceBuilder,
        SearchRequestBody protoRequest,
        AbstractQueryBuilderProtoUtils queryUtils
    ) throws IOException {
        // Parse all non-query fields
        parseNonQueryFields(searchSourceBuilder, protoRequest, queryUtils.getRegistry());

        // Handle queries using the instance-based approach
        if (protoRequest.hasQuery()) {
            searchSourceBuilder.query(queryUtils.parseInnerQueryBuilderProto(protoRequest.getQuery()));
        }
        if (protoRequest.hasPostFilter()) {
            searchSourceBuilder.postFilter(queryUtils.parseInnerQueryBuilderProto(protoRequest.getPostFilter()));
        }
    }

    /**
     * Parses all fields except queries from the protobuf SearchRequestBody.
     */
    private static void parseNonQueryFields(
        SearchSourceBuilder searchSourceBuilder,
        SearchRequestBody protoRequest,
        QueryBuilderProtoConverterRegistry registry
    ) throws IOException {
        // TODO what to do about parser.getDeprecationHandler() for protos?

        if (protoRequest.hasFrom()) {
            searchSourceBuilder.from(protoRequest.getFrom());
        }
        if (protoRequest.hasSize()) {
            searchSourceBuilder.size(protoRequest.getSize());
        }
        if (protoRequest.hasTimeout()) {
            searchSourceBuilder.timeout(TimeValue.parseTimeValue(protoRequest.getTimeout(), null, TIMEOUT_FIELD.getPreferredName()));
        }
        if (protoRequest.hasTerminateAfter()) {
            searchSourceBuilder.terminateAfter(protoRequest.getTerminateAfter());
        }
        if (protoRequest.hasMinScore()) {
            searchSourceBuilder.minScore(protoRequest.getMinScore());
        }
        if (protoRequest.hasVersion()) {
            searchSourceBuilder.version(protoRequest.getVersion());
        }
        if (protoRequest.hasSeqNoPrimaryTerm()) {
            searchSourceBuilder.seqNoAndPrimaryTerm(protoRequest.getSeqNoPrimaryTerm());
        }
        if (protoRequest.hasExplain()) {
            searchSourceBuilder.explain(protoRequest.getExplain());
        }
        if (protoRequest.hasTrackScores()) {
            searchSourceBuilder.trackScores(protoRequest.getTrackScores());
        }
        if (protoRequest.hasIncludeNamedQueriesScore()) {
            searchSourceBuilder.includeNamedQueriesScores(protoRequest.getIncludeNamedQueriesScore());
        }
        if (protoRequest.hasTrackTotalHits()) {
            if (protoRequest.getTrackTotalHits().getTrackHitsCase() == TrackHits.TrackHitsCase.ENABLED) {
                searchSourceBuilder.trackTotalHitsUpTo(
                    protoRequest.getTrackTotalHits().getEnabled() ? TRACK_TOTAL_HITS_ACCURATE : TRACK_TOTAL_HITS_DISABLED
                );
            } else if (protoRequest.getTrackTotalHits().getTrackHitsCase() == TrackHits.TrackHitsCase.COUNT) {
                searchSourceBuilder.trackTotalHitsUpTo(protoRequest.getTrackTotalHits().getCount());
            }
        }
        if (protoRequest.hasXSource()) {
            searchSourceBuilder.fetchSource(FetchSourceContextProtoUtils.fromProto(protoRequest.getXSource()));
        }
        if (protoRequest.getStoredFieldsCount() > 0) {
            searchSourceBuilder.storedFields(StoredFieldsContextProtoUtils.fromProto(protoRequest.getStoredFieldsList()));
        }
        if (protoRequest.getSortCount() > 0) {
            for (SortBuilder<?> sortBuilder : SortBuilderProtoUtils.fromProto(protoRequest.getSortList(), registry)) {
                searchSourceBuilder.sort(sortBuilder);
            }
        }
        if (protoRequest.hasProfile()) {
            searchSourceBuilder.profile(protoRequest.getProfile());
        }
        if (protoRequest.hasSearchPipeline()) {
            searchSourceBuilder.pipeline(protoRequest.getSearchPipeline());
        }
        if (protoRequest.hasVerbosePipeline()) {
            searchSourceBuilder.verbosePipeline(protoRequest.getVerbosePipeline());
        }
        if (protoRequest.hasXSource()) {
            searchSourceBuilder.fetchSource(FetchSourceContextProtoUtils.fromProto(protoRequest.getXSource()));
        }
        if (protoRequest.getScriptFieldsCount() > 0) {
            for (Map.Entry<String, ScriptField> entry : protoRequest.getScriptFieldsMap().entrySet()) {
                String name = entry.getKey();
                ScriptField scriptFieldProto = entry.getValue();
                SearchSourceBuilder.ScriptField scriptField = ScriptFieldProtoUtils.fromProto(name, scriptFieldProto);
                searchSourceBuilder.scriptField(name, scriptField.script(), scriptField.ignoreFailure());
            }
        }
        if (protoRequest.getIndicesBoostCount() > 0) {
            for (Map.Entry<String, Float> entry : protoRequest.getIndicesBoostMap().entrySet()) {
                searchSourceBuilder.indexBoost(entry.getKey(), entry.getValue());
            }
        }

        // Aggregations field was removed in protobufs 1.0.0
        // TODO: Support aggregations when they are re-added to the proto
        /*
        if (protoRequest.getAggregationsCount() > 0) {
            throw new UnsupportedOperationException("aggregations param is not supported yet");
        }
        */
        if (protoRequest.hasHighlight()) {
            searchSourceBuilder.highlighter(HighlightBuilderProtoUtils.fromProto(protoRequest.getHighlight(), registry));
        }

        // TODO support suggest once added back to the protos
        /*
        if (protoRequest.hasSuggest()) {
            throw new UnsupportedOperationException("suggest param is not supported yet");
            // searchSourceBuilder.suggest(SuggestBuilderProtoUtils.fromProto(protoRequest.getSuggest()));
        }
        */
        if (protoRequest.getRescoreCount() > 0) {
            for (Rescore rescore : protoRequest.getRescoreList()) {
                searchSourceBuilder.addRescorer(RescorerBuilderProtoUtils.parseFromProto(rescore));
            }
        }

        if (protoRequest.hasExt()) {
            // TODO support ext
            throw new UnsupportedOperationException("ext param is not supported yet");
        }
        if (protoRequest.hasSlice()) {
            searchSourceBuilder.slice(SliceBuilderProtoUtils.fromProto(protoRequest.getSlice()));
        }
        if (protoRequest.hasCollapse()) {
            searchSourceBuilder.collapse(CollapseBuilderProtoUtils.fromProto(protoRequest.getCollapse(), registry));
        }
        if (protoRequest.hasPit()) {
            searchSourceBuilder.pointInTimeBuilder(PointInTimeBuilderProtoUtils.fromProto(protoRequest.getPit()));
        }
        if (protoRequest.getDerivedCount() > 0) {
            for (Map.Entry<String, DerivedField> entry : protoRequest.getDerivedMap().entrySet()) {
                String name = entry.getKey();
                DerivedField derivedFieldProto = entry.getValue();

                // Convert protobuf DerivedField to OpenSearch DerivedField using the REST side pattern
                // This uses simple constructor + conditional setters (matches DerivedFieldMapper.Builder.build())
                org.opensearch.index.mapper.DerivedField derivedField = DerivedFieldProtoUtils.fromProto(name, derivedFieldProto);

                // Add to SearchSourceBuilder - check if any optional fields are set to choose the right method
                if (derivedField.getProperties() != null
                    || derivedField.getPrefilterField() != null
                    || derivedField.getFormat() != null
                    || derivedField.getIgnoreMalformed()) {
                    // Use full constructor when optional fields are present
                    searchSourceBuilder.derivedField(
                        derivedField.getName(),
                        derivedField.getType(),
                        derivedField.getScript(),
                        derivedField.getProperties(),
                        derivedField.getPrefilterField(),
                        derivedField.getFormat(),
                        derivedField.getIgnoreMalformed() ? Boolean.TRUE : null
                    );
                } else {
                    // Use simple constructor when no optional fields
                    searchSourceBuilder.derivedField(derivedField.getName(), derivedField.getType(), derivedField.getScript());
                }
            }
        }
        if (protoRequest.getDocvalueFieldsCount() > 0) {
            for (FieldAndFormat fieldAndFormatProto : protoRequest.getDocvalueFieldsList()) {
                /**
                 * Similar to {@link org.opensearch.search.fetch.subphase.FieldAndFormat#fromXContent(XContentParser)}
                 */
                searchSourceBuilder.docValueField(fieldAndFormatProto.getField(), fieldAndFormatProto.getFormat());
            }

        }
        if (protoRequest.getFieldsCount() > 0) {
            for (FieldAndFormat fieldAndFormatProto : protoRequest.getFieldsList()) {
                /**
                 * Similar to {@link org.opensearch.search.fetch.subphase.FieldAndFormat#fromXContent(XContentParser)}
                 */
                searchSourceBuilder.fetchField(fieldAndFormatProto.getField(), fieldAndFormatProto.getFormat());
            }
        }
        if (protoRequest.getStatsCount() > 0) {
            searchSourceBuilder.stats(protoRequest.getStatsList());
        }
        if (protoRequest.getSearchAfterCount() > 0) {
            searchSourceBuilder.searchAfter(SearchAfterBuilderProtoUtils.fromProto(protoRequest.getSearchAfterList()));
        }
    }

    /**
     * Utility class for converting ScriptField Protocol Buffers to OpenSearch objects.
     * This class handles the transformation of script field definitions between the two formats.
     */
    public static class ScriptFieldProtoUtils {
        /**
         * Private constructor to prevent instantiation.
         * This is a utility class with only static methods.
         */
        private ScriptFieldProtoUtils() {
            // Utility class, no instances
        }

        /**
         * Similar to {@link SearchSourceBuilder.ScriptField#ScriptField(XContentParser)}
         *
         * @param scriptFieldName
         * @param scriptFieldProto
         * @throws IOException if there's an error during parsing
         */

        public static SearchSourceBuilder.ScriptField fromProto(String scriptFieldName, ScriptField scriptFieldProto) throws IOException {
            org.opensearch.script.Script script = ScriptProtoUtils.parseFromProtoRequest(scriptFieldProto.getScript());
            boolean ignoreFailure = scriptFieldProto.hasIgnoreFailure() ? scriptFieldProto.getIgnoreFailure() : false;

            return new SearchSourceBuilder.ScriptField(scriptFieldName, script, ignoreFailure);
        }

    }
}
