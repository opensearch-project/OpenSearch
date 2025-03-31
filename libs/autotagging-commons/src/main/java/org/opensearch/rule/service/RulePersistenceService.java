/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.autotagging.Attribute;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule;
import org.opensearch.autotagging.Rule.Builder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.*;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * This class encapsulates the logic to manage the lifecycle of rules at index level
 * @opensearch.experimental
 */
public abstract class RulePersistenceService {
    /**
     * The system index name used for storing rules
     */
    public static final String RULES_INDEX = ".rules";
    private final Client client;
    private final ClusterService clusterService;
    private static final Logger logger = LogManager.getLogger(RulePersistenceService.class);

    /**
     * The maximum number of results allowed per GET request
     */
    public static final int MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST = 50;

    @Inject
    public RulePersistenceService(final ClusterService clusterService, final Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Entry point for the get rule api logic in persistence service. If id is provided, we only get a single rule.
     * Otherwise, we get all rules that satisfy the attributeFilters.
     * @param id - The id of the rule to get.
     * @param attributeFilters - A map containing the attributes that user want to filter on
     * @param searchAfter - The sort values from the last document of the previous page, used for pagination
     * @param listener - ActionListener for GetRuleResponse
     */
    public void getRule(
        String id,
        Map<Attribute, Set<String>> attributeFilters,
        String searchAfter,
        ActionListener<? extends GetRuleResponse> listener
    ) {
        // Stash the current thread context when interacting with system index to perform
        // operations as the system itself, bypassing authorization checks. This ensures that
        // actions within this block are trusted and executed with system-level privileges.
        try (ThreadContext.StoredContext context = getContext()) {
            BoolQueryBuilder boolQuery = buildGetRuleQuery(id, attributeFilters);
            SearchRequestBuilder searchRequest = client.prepareSearch(RULES_INDEX)
                .setQuery(boolQuery)
                .setSize(getMaxReturnSizeAllowedPerGetRequest());
            if (searchAfter != null) {
                searchRequest.addSort(_ID_STRING, SortOrder.ASC).searchAfter(new Object[] { searchAfter });
            }
            searchRequest.execute(ActionListener.wrap(searchResponse -> handleGetRuleResponse(id, searchResponse, listener), e -> {
                logger.error("Failed to fetch all rules: {}", e.getMessage());
                listener.onFailure(e);
            }));
        }
    }

    /**
     * Builds a bool query to retrieve rules from the rules index, applying attribute-based filters
     * when needed and ensuring that the rules are associated with the query group feature type.
     * @param id              The ID of the rule to fetch. If not null, the search will return only this specific rule.
     * @param attributeFilters A map of attributes to their associated set of values used to filter the rules.
     */
    public BoolQueryBuilder buildGetRuleQuery(String id, Map<Attribute, Set<String>> attributeFilters) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (id != null) {
            return boolQuery.must(QueryBuilders.termQuery(_ID_STRING, id));
        }
        for (Map.Entry<Attribute, Set<String>> entry : attributeFilters.entrySet()) {
            Attribute attribute = entry.getKey();
            Set<String> values = entry.getValue();
            if (values != null && !values.isEmpty()) {
                BoolQueryBuilder attributeQuery = QueryBuilders.boolQuery();
                for (String value : values) {
                    attributeQuery.should(QueryBuilders.matchQuery(attribute.getName(), value));
                }
                boolQuery.must(attributeQuery);
            }
        }
        boolQuery.filter(QueryBuilders.existsQuery(retrieveFeatureTypeInstance().getName()));
        return boolQuery;
    }

    /**
     * Process searchResponse from index and send a GetRuleResponse
     * @param searchResponse - Response received from index
     * @param listener - ActionListener for GetRuleResponse
     */
    void handleGetRuleResponse(String id, SearchResponse searchResponse, ActionListener<? extends GetRuleResponse> listener) {
        List<SearchHit> hits = Arrays.asList(searchResponse.getHits().getHits());
        if (id != null && hits.isEmpty()) {
            logger.error("Rule with ID " + id + " not found.");
            listener.onFailure(new ResourceNotFoundException("Rule with ID " + id + " doesn't exist in the .rules index."));
            return;
        }
        Map<String, Rule> ruleMap = hits.stream()
            .map(hit -> parseRule(hit.getId(), hit.getSourceAsString()))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        String nextSearchAfter = hits.isEmpty() ? null : hits.get(hits.size() - 1).getId();
        listener.onResponse(buildGetRuleResponse(ruleMap, nextSearchAfter, RestStatus.OK));
    }

    /**
     * Parses a source string into a Rule object
     * @param id - document id for the Rule object
     * @param source - The raw source string representing the rule to be parsed
     */
    private Map.Entry<String, Rule> parseRule(String id, String source) {
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source)
        ) {
            return Map.entry(id, Builder.fromXContent(parser, retrieveFeatureTypeInstance()).build());
        } catch (IOException e) {
            logger.info("Issue met when parsing rule for ID {}: {}", id, e.getMessage());
            return null;
        }
    }

    private boolean isExistingQueryGroup(String queryGroupId) {
        return clusterService.state().metadata().queryGroups().containsKey(queryGroupId);
    }

    /**
     * Abstract method for subclasses to provide specific FeatureType Instance
     */
    protected abstract FeatureType retrieveFeatureTypeInstance();

    protected abstract <T extends GetRuleResponse> T buildGetRuleResponse(Map<String, Rule> ruleMap, String nextSearchAfter, RestStatus restStatus);

    private ThreadContext.StoredContext getContext() {
        return client.threadPool().getThreadContext().stashContext();
    }

    public int getMaxReturnSizeAllowedPerGetRequest() {
        return MAX_RETURN_SIZE_ALLOWED_PER_GET_REQUEST;
    }

    public Client getClient() {
        return client;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }
}
