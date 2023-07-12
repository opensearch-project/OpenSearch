/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.io.InputStream;
import java.util.Map;

import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * This is a {@link SearchRequestProcessor} that replaces the incoming query with a BooleanQuery
 * that MUST match the incoming query with a FILTER clause based on the configured query.
 */
public class FilterQueryRequestProcessor extends AbstractProcessor implements SearchRequestProcessor {
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "filter_query";

    final QueryBuilder filterQuery;

    /**
     * Returns the type of the processor.
     *
     * @return The processor type.
     */
    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Constructor that takes a filter query.
     *
     * @param tag            processor tag
     * @param description    processor description
     * @param ignoreFailure  option to ignore failure
     * @param filterQuery the query that will be added as a filter to incoming queries
     */
    FilterQueryRequestProcessor(String tag, String description, boolean ignoreFailure, QueryBuilder filterQuery) {
        super(tag, description, ignoreFailure);
        this.filterQuery = filterQuery;
    }

    /**
     * Modifies the search request by adding a filtered query to the existing query, if any, and sets it as the new query
     * in the search request's SearchSourceBuilder.
     *
     * @param request The search request to be processed.
     * @return The modified search request.
     * @throws Exception if an error occurs while processing the request.
     */
    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        QueryBuilder originalQuery = null;
        if (request.source() != null) {
            originalQuery = request.source().query();
        }

        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(filterQuery);
        if (originalQuery != null) {
            filteredQuery.must(originalQuery);
        }
        if (request.source() == null) {
            request.source(new SearchSourceBuilder());
        }
        request.source().query(filteredQuery);
        return request;
    }

    static class Factory implements Processor.Factory<SearchRequestProcessor> {
        private static final String QUERY_KEY = "query";
        private final NamedXContentRegistry namedXContentRegistry;

        Factory(NamedXContentRegistry namedXContentRegistry) {
            this.namedXContentRegistry = namedXContentRegistry;
        }

        @Override
        public FilterQueryRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            Map<String, Object> query = ConfigurationUtils.readOptionalMap(TYPE, tag, config, QUERY_KEY);
            if (query == null) {
                throw new IllegalArgumentException("Did not specify the " + QUERY_KEY + " property in processor of type " + TYPE);
            }
            try (
                XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent).map(query);
                InputStream stream = BytesReference.bytes(builder).streamInput();
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, stream)
            ) {
                return new FilterQueryRequestProcessor(tag, description, ignoreFailure, parseInnerQueryBuilder(parser));
            }
        }
    }
}
