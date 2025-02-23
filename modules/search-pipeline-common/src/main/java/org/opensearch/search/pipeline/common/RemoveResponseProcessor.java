/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.common.helpers.SearchResponseUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Processor that removes existing fields from search hit.
 * When ignoreMissing is true, nothing happens if the field is not present.
 */
public class RemoveResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    public static final String TYPE = "remove";

    private final boolean removeAll;
    private final boolean ignoreMissing;
    private final List<String> remove_fields;
    static final String REMOVE_FIELD_ARRAY = "fields";
    static final String REMOVE_ALL = "remove_all_hits";
    static final String IGNORE_MISSING = "ignore_missing";

    /**
     * Constructs a new RemoveResponseProcessor.
     *
     * @param tag           The processor's tag.
     * @param description   A description of the processor.
     * @param ignoreFailure Whether to ignore failures during processing.
     * @param removeAll     If true, removes all fields from each hit.
     * @param ignoreMissing If true, ignores fields that are not present in the hit.
     * @param fields        A list of field names to remove.
     */
    protected RemoveResponseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        boolean removeAll,
        boolean ignoreMissing,
        List<String> fields
    ) {
        super(tag, description, ignoreFailure);
        this.ignoreMissing = ignoreMissing;
        this.remove_fields = fields;
        this.removeAll = removeAll;
    }

    /**
     * Gets the type of processor
     */
    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Processes the search response by removing specified fields from each hit.
     * If {@code removeAll} is true, it removes all fields and the source from each hit.
     * Otherwise, it removes only the specified fields, including nested fields.
     *
     * @param request  The original search request.
     * @param response The search response to process.
     * @return The processed search response.
     * @throws Exception If an error occurs during processing.
     */
    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        boolean foundField = false;
        if (response.getHits() == null) {
            return response;
        }

        SearchHit[] hits = response.getHits().getHits();

        if (this.removeAll) {
            return SearchResponseUtil.replaceHits(SearchHits.empty(), response);
        } else {
            for (SearchHit hit : hits) {
                Map<String, DocumentField> fields = hit.getFields();
                for (String field : this.remove_fields) {
                    if (fields.containsKey(field)) {
                        foundField = true;
                        hit.removeDocumentField(field);
                    }
                    if (hit.hasSource()) {
                        try {
                            BytesReference sourceRef = hit.getSourceRef();
                            Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper.convertToMap(
                                sourceRef,
                                false,
                                (MediaType) null
                            );

                            Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
                            if (sourceAsMap.containsKey(field)) {
                                foundField = true;
                                sourceAsMap.remove(field);
                                XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                                builder.map(sourceAsMap);
                                hit.sourceRef(BytesReference.bytes(builder));
                            }

                            if (!foundField && !ignoreMissing) {
                                throw new IllegalArgumentException("Field [" + field + "] doesn't exist in document");
                            }
                        } catch (IOException e) {
                            throw new IllegalArgumentException("Failed to parse source", e);
                        }
                    }

                }
            }
        }

        return response;
    }

    /**
     * Factory class for creating RemoveResponseProcessor instances.
     */
    static class Factory implements Processor.Factory<SearchResponseProcessor> {
        /**
         * Creates a new RemoveResponseProcessor based on the provided configuration.
         *
         * @param processorFactories Map of processor factories.
         * @param tag                The processor's tag.
         * @param description        A description of the processor.
         * @param ignoreFailure      Whether to ignore failures during processing.
         * @param config             The configuration map for the processor.
         * @param pipelineContext    The context of the pipeline.
         * @return A new RemoveResponseProcessor instance.
         */
        @Override
        public RemoveResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            List<String> removeFields = ConfigurationUtils.readOptionalList(TYPE, tag, config, REMOVE_FIELD_ARRAY);
            boolean removeAll = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, REMOVE_ALL, false);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, IGNORE_MISSING, false);
            return new RemoveResponseProcessor(tag, description, ignoreFailure, removeAll, ignoreMissing, removeFields);
        }
    }
}
