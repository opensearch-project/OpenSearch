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
import org.opensearch.common.document.DocumentField;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.common.helpers.SearchResponseUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A simple implementation of field collapsing on search responses. Note that this is not going to work as well as
 * field collapsing at the shard level, as implemented with the "collapse" parameter in a search request. Mostly
 * just using this to demo the oversample / truncate_hits processors.
 */
public class CollapseResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "collapse";
    static final String COLLAPSE_FIELD = "field";
    private final String collapseField;

    private CollapseResponseProcessor(String tag, String description, boolean ignoreFailure, String collapseField) {
        super(tag, description, ignoreFailure);
        this.collapseField = Objects.requireNonNull(collapseField);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {

        if (response.getHits() != null) {
            if (response.getHits().getCollapseField() != null) {
                throw new IllegalStateException(
                    "Cannot collapse on " + collapseField + ". Results already collapsed on " + response.getHits().getCollapseField()
                );
            }
            Map<String, SearchHit> collapsedHits = new LinkedHashMap<>();
            List<Object> collapseValues = new ArrayList<>();
            for (SearchHit hit : response.getHits()) {
                Object fieldValue = null;
                DocumentField docField = hit.getFields().get(collapseField);
                if (docField != null) {
                    if (docField.getValues().size() > 1) {
                        throw new IllegalStateException(
                            "Failed to collapse " + hit.getId() + ": doc has multiple values for field " + collapseField
                        );
                    }
                    fieldValue = docField.getValues().get(0);
                } else if (hit.getSourceAsMap() != null) {
                    fieldValue = hit.getSourceAsMap().get(collapseField);
                }
                String fieldValueString;
                if (fieldValue == null) {
                    fieldValueString = "__missing__";
                } else {
                    fieldValueString = fieldValue.toString();
                }

                // Results are already sorted by sort criterion. Only keep the first hit for each field.
                if (collapsedHits.containsKey(fieldValueString) == false) {
                    collapsedHits.put(fieldValueString, hit);
                    collapseValues.add(fieldValue);
                }
            }
            SearchHit[] newHits = new SearchHit[collapsedHits.size()];
            int i = 0;
            for (SearchHit collapsedHit : collapsedHits.values()) {
                newHits[i++] = collapsedHit;
            }
            SearchHits searchHits = new SearchHits(
                newHits,
                response.getHits().getTotalHits(),
                response.getHits().getMaxScore(),
                response.getHits().getSortFields(),
                collapseField,
                collapseValues.toArray()
            );
            return SearchResponseUtil.replaceHits(searchHits, response);
        }
        return response;
    }

    static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public CollapseResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            String collapseField = ConfigurationUtils.readStringProperty(TYPE, tag, config, COLLAPSE_FIELD);
            return new CollapseResponseProcessor(tag, description, ignoreFailure, collapseField);
        }
    }

}
