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
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Map;

/**
 * This is a {@link SearchRequestProcessor} that renames a field before returning the search response
 */
public class DeleteFieldResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {

    private final String field;
    private final boolean ignoreMissing;

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "delete_field";

    /**
     * Constructor that takes a target field to be deleted
     *
     * @param tag            processor tag
     * @param description    processor description
     * @param ignoreFailure  option to ignore failure
     * @param field        name of field to delete
     * @param ignoreMissing if true, do not throw error if oldField does not exist within search response
     */
    public DeleteFieldResponseProcessor(String tag, String description, boolean ignoreFailure, String field, boolean ignoreMissing) {
        super(tag, description, ignoreFailure);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Get the provided field to be deleted
     * @return oldField
     */
    public String getField() {
        return field;
    }

    /**
     * Getter function for ignoreMissing
     * @return ignoreMissing
     */
    public boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        boolean foundField = false;

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, DocumentField> fields = hit.getFields();
            if (fields.containsKey(field)) {
                foundField = true;
                DocumentField old = hit.removeDocumentField(field);
            }

            if (hit.hasSource()) {
                BytesReference sourceRef = hit.getSourceRef();
                Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper.convertToMap(
                    sourceRef,
                    false,
                    (MediaType) null
                );

                Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
                if (sourceAsMap.containsKey(field)) {
                    foundField = true;
                    Object val = sourceAsMap.remove(field);
                    XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                    builder.map(sourceAsMap);
                    hit.sourceRef(BytesReference.bytes(builder));
                }
            }

            if (!foundField && !ignoreMissing) {
                throw new IllegalArgumentException("Document with id " + hit.getId() + " is missing field " + field);
            }
        }

        return response;
    }

    /**
     * This is a factory that creates the DeleteFieldResponseProcessor
     */
    public static final class Factory implements Processor.Factory<SearchResponseProcessor> {

        /**
         * Constructor for factory
         */
        Factory() {}

        @Override
        public DeleteFieldResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            return new DeleteFieldResponseProcessor(tag, description, ignoreFailure, field, ignoreMissing);
        }
    }
}
