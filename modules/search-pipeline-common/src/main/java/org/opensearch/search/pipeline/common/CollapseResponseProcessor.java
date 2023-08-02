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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String COLLAPSE_FIELD = "field";
    private final String collapseField;

    private CollapseResponseProcessor(String tag, String description, boolean ignoreFailure, String collapseField) {
        super(tag, description, ignoreFailure);
        this.collapseField = collapseField;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {

        if (response.getHits() != null) {
            Map<String, SearchHit> collapsedHits = new HashMap<>();
            for (SearchHit hit : response.getHits()) {
                String fieldValue = "<missing>";
                DocumentField docField = hit.getFields().get(collapseField);
                if (docField != null) {
                    if (docField.getValues().size() > 1) {
                        throw new IllegalStateException("Document " + hit.getId() + " has multiple values for field " + collapseField);
                    }
                    fieldValue = docField.getValues().get(0).toString();
                } else if (hit.hasSource()) {
                    Object val = hit.getSourceAsMap().get(collapseField);
                    if (val != null) {
                        fieldValue = val.toString();
                    }
                }
                SearchHit previousHit = collapsedHits.get(fieldValue);
                // TODO - Support the sort used in the request, rather than just score
                if (previousHit == null || hit.getScore() > previousHit.getScore()) {
                    collapsedHits.put(fieldValue, hit);
                }
            }
            List<SearchHit> hitsToReturn = new ArrayList<>(collapsedHits.values());
            hitsToReturn.sort(Comparator.comparingDouble(SearchHit::getScore).reversed());
            SearchHit[] newHits = hitsToReturn.toArray(new SearchHit[0]);
            List<String> collapseValues = new ArrayList<>(collapsedHits.keySet());
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
