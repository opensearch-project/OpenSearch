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
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Processor that sorts an array of items.
 * Throws exception is the specified field is not an array.
 */
public class SplitResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    /** Key to reference this processor type from a search pipeline. */
    public static final String TYPE = "split";
    /** Key defining the string field to be split. */
    public static final String SPLIT_FIELD = "field";
    /** Key defining the delimiter used to split the string. This can be a regular expression pattern. */
    public static final String SEPARATOR = "separator";
    /** Optional key for handling empty trailing fields. */
    public static final String PRESERVE_TRAILING = "preserve_trailing";
    /** Optional key to put the split values in a different field. */
    public static final String TARGET_FIELD = "target_field";

    private final String splitField;
    private final String separator;
    private final boolean preserveTrailing;
    private final String targetField;

    SplitResponseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String splitField,
        String separator,
        boolean preserveTrailing,
        String targetField
    ) {
        super(tag, description, ignoreFailure);
        this.splitField = Objects.requireNonNull(splitField);
        this.separator = Objects.requireNonNull(separator);
        this.preserveTrailing = preserveTrailing;
        this.targetField = targetField == null ? splitField : targetField;
    }

    /**
     * Getter function for splitField
     * @return sortField
     */
    public String getSplitField() {
        return splitField;
    }

    /**
     * Getter function for separator
     * @return separator
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * Getter function for preserveTrailing
     * @return preserveTrailing;
     */
    public boolean isPreserveTrailing() {
        return preserveTrailing;
    }

    /**
     * Getter function for targetField
     * @return targetField
     */
    public String getTargetField() {
        return targetField;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
            Map<String, DocumentField> fields = hit.getFields();
            if (fields.containsKey(splitField)) {
                DocumentField docField = hit.getFields().get(splitField);
                if (docField == null) {
                    throw new IllegalArgumentException("field [" + splitField + "] is null, cannot split.");
                }
                Object val = docField.getValue();
                if (val == null || !String.class.isAssignableFrom(val.getClass())) {
                    throw new IllegalArgumentException("field [" + splitField + "] is not a string, cannot split");
                }
                String[] strings = ((String) val).split(separator, preserveTrailing ? -1 : 0);
                List<Object> splitList = Stream.of(strings).collect(Collectors.toList());
                hit.setDocumentField(targetField, new DocumentField(targetField, splitList));
            }
            if (hit.hasSource()) {
                BytesReference sourceRef = hit.getSourceRef();
                Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper.convertToMap(
                    sourceRef,
                    false,
                    (MediaType) null
                );

                Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
                if (sourceAsMap.containsKey(splitField)) {
                    Object val = sourceAsMap.get(splitField);
                    if (val instanceof String) {
                        String[] strings = ((String) val).split(separator, preserveTrailing ? -1 : 0);
                        List<Object> splitList = Stream.of(strings).collect(Collectors.toList());
                        sourceAsMap.put(targetField, splitList);
                    }
                    XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                    builder.map(sourceAsMap);
                    hit.sourceRef(BytesReference.bytes(builder));
                }
            }
        }
        return response;
    }

    static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public SplitResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            String splitField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "field");
            String separator = ConfigurationUtils.readStringProperty(TYPE, tag, config, "separator");
            boolean preserveTrailing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "preserve_trailing", false);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field", splitField);
            return new SplitResponseProcessor(tag, description, ignoreFailure, splitField, separator, preserveTrailing, targetField);
        }
    }
}
