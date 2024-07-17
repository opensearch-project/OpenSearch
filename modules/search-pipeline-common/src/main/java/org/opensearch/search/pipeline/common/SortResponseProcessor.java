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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Processor that sorts an array of items.
 * Throws exception is the specified field is not an array.
 */
public class SortResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "sort";
    public static final String SORT_FIELD = "field";
    public static final String SORT_ORDER = "order";
    public static final String TARGET_FIELD = "target_field";
    public static final String DEFAULT_ORDER = "asc";

    public enum SortOrder {
        ASCENDING("asc"),
        DESCENDING("desc");

        private final String direction;

        SortOrder(String direction) {
            this.direction = direction;
        }

        @Override
        public String toString() {
            return this.direction;
        }

        public static SortOrder fromString(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Sort direction cannot be null");
            }

            if (value.equals(ASCENDING.toString())) {
                return ASCENDING;
            } else if (value.equals(DESCENDING.toString())) {
                return DESCENDING;
            }
            throw new IllegalArgumentException("Sort direction [" + value + "] not recognized." + " Valid values are: [asc, desc]");
        }
    }

    private final String sortField;
    private final SortOrder sortOrder;
    private final String targetField;

    SortResponseProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String sortField,
        SortOrder sortOrder,
        String targetField
    ) {
        super(tag, description, ignoreFailure);
        this.sortField = Objects.requireNonNull(sortField);
        this.sortOrder = Objects.requireNonNull(sortOrder);
        this.targetField = targetField == null ? sortField : targetField;
    }

    /**
     * Getter function for sortField
     * @return sortField
     */
    public String getSortField() {
        return sortField;
    }

    /**
     * Getter function for targetField
     * @return targetField
     */
    public String getTargetField() {
        return targetField;
    }

    /**
     * Getter function for sortOrder
     * @return sortOrder
     */
    public SortOrder getSortOrder() {
        return sortOrder;
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
            if (fields.containsKey(sortField)) {
                DocumentField docField = hit.getFields().get(sortField);
                if (docField == null) {
                    throw new IllegalArgumentException("field [" + sortField + "] is null, cannot sort.");
                }
                hit.setDocumentField(targetField, new DocumentField(targetField, getSortedValues(docField.getValues())));
            }
            if (hit.hasSource()) {
                BytesReference sourceRef = hit.getSourceRef();
                Tuple<? extends MediaType, Map<String, Object>> typeAndSourceMap = XContentHelper.convertToMap(
                    sourceRef,
                    false,
                    (MediaType) null
                );

                Map<String, Object> sourceAsMap = typeAndSourceMap.v2();
                if (sourceAsMap.containsKey(sortField)) {
                    Object val = sourceAsMap.get(sortField);
                    if (val instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> listVal = (List<Object>) val;
                        sourceAsMap.put(targetField, getSortedValues(listVal));
                    }
                    XContentBuilder builder = XContentBuilder.builder(typeAndSourceMap.v1().xContent());
                    builder.map(sourceAsMap);
                    hit.sourceRef(BytesReference.bytes(builder));
                }
            }
        }
        return response;
    }

    private <T extends Comparable<T>> List<Object> getSortedValues(List<Object> values) {
        List<T> comparableValues = new ArrayList<>(values.size());
        for (Object obj : values) {
            if (obj == null) {
                throw new IllegalArgumentException("field [" + sortField + "] contains a null value.]");
            } else if (Comparable.class.isAssignableFrom(obj.getClass())) {
                @SuppressWarnings("unchecked")
                T comp = (T) obj;
                comparableValues.add(comp);
            } else {
                throw new IllegalArgumentException(
                    "field [" + sortField + "] of type [" + obj.getClass().getName() + "] is not comparable.]"
                );
            }
        }
        return comparableValues.stream()
            .sorted(sortOrder.equals(SortOrder.ASCENDING) ? Comparator.naturalOrder() : Comparator.reverseOrder())
            .collect(Collectors.toList());
    }

    static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public SortResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            String sortField = ConfigurationUtils.readStringProperty(TYPE, tag, config, SORT_FIELD);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, TARGET_FIELD, sortField);
            try {
                SortOrder sortOrder = SortOrder.fromString(
                    ConfigurationUtils.readStringProperty(TYPE, tag, config, SORT_ORDER, DEFAULT_ORDER)
                );
                return new SortResponseProcessor(tag, description, ignoreFailure, sortField, sortOrder, targetField);
            } catch (IllegalArgumentException e) {
                throw ConfigurationUtils.newConfigurationException(TYPE, tag, SORT_ORDER, e.getMessage());
            }
        }
    }
}
