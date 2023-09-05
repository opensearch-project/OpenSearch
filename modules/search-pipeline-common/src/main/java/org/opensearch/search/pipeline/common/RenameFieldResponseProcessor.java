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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Map;

/**
 * This is a {@link SearchRequestProcessor} that renames a field before returning the search response
 */
public class RenameFieldResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {

    private final String oldField;
    private final String newField;
    private final boolean ignoreMissing;

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "rename_field";

    /**
     * Constructor that takes a target field to rename and the new name
     *
     * @param tag            processor tag
     * @param description    processor description
     * @param ignoreFailure  option to ignore failure
     * @param oldField       name of field to be renamed
     * @param newField       name of field that will replace the old field
     * @param ignoreMissing if true, do not throw error if oldField does not exist within search response
     */
    public RenameFieldResponseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String oldField,
        String newField,
        boolean ignoreMissing
    ) {
        super(tag, description, ignoreFailure);
        this.oldField = oldField;
        this.newField = newField;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Getter function for oldField
     * @return oldField
     */
    public String getOldField() {
        return oldField;
    }

    /**
     * Getter function for newField
     * @return newField
     */
    public String getNewField() {
        return newField;
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
            if (fields.containsKey(oldField)) {
                foundField = true;
                DocumentField old = hit.removeDocumentField(oldField);
                DocumentField newDocField = new DocumentField(newField, old.getValues());
                hit.setDocumentField(newField, newDocField);
            }

            if (hit.hasSource()) {
                BytesReference sourceRef = hit.getSourceRef();
                Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper.convertToMap(
                    sourceRef,
                    false,
                    (MediaType) null
                );

                Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
                if (sourceAsMap.containsKey(oldField)) {
                    foundField = true;
                    Object val = sourceAsMap.remove(oldField);
                    sourceAsMap.put(newField, val);

                    XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                    builder.map(sourceAsMap);
                    hit.sourceRef(BytesReference.bytes(builder));
                }
            }

            if (!foundField && !ignoreMissing) {
                throw new IllegalArgumentException("Document with id " + hit.getId() + " is missing field " + oldField);
            }
        }

        return response;
    }

    /**
     * This is a factor that creates the RenameResponseProcessor
     */
    public static final class Factory implements Processor.Factory<SearchResponseProcessor> {

        /**
         * Constructor for factory
         */
        Factory() {}

        @Override
        public RenameFieldResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            String oldField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String newField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            return new RenameFieldResponseProcessor(tag, description, ignoreFailure, oldField, newField, ignoreMissing);
        }
    }
}
