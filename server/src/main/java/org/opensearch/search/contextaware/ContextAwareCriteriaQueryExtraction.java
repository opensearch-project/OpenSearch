/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.contextaware;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.ContextAwareGroupingFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WithFilterQueryBuilder;
import org.opensearch.script.ContextAwareGroupingScript;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts and manages context-aware criteria from queries for specialized segment-level search operations.
 * This class provides functionality to analyze queries and extract criteria information that can be used
 * for context-aware searching.
 *
 * <p>Context-aware searching allows for efficient segment-level filtering based on predefined criteria.
 *
 * @since 3.4
 */
@ExperimentalApi
public class ContextAwareCriteriaQueryExtraction {

    private final MapperService mapperService;
    private ContextAwareGroupingScript contextAwareGroupingScript;
    private String contextAwareGroupingFieldName;

    /**
     * Constructs a new ContextAwareCriteriaQueryExtraction instance.
     *
     * <p>Extracts the field name and script from the {@link ContextAwareGroupingFieldMapper}.
     *
     * @param mapperService The MapperService instance used to access field mappers.
     */
    public ContextAwareCriteriaQueryExtraction(final MapperService mapperService) {
        this.mapperService = mapperService;
        if (mapperService.documentMapper() != null && mapperService.documentMapper().mappers() != null) {
            final Mapper mapper = mapperService.documentMapper().mappers().getMapper(ContextAwareGroupingFieldMapper.CONTENT_TYPE);
            if (mapper != null) {
                final ContextAwareGroupingFieldMapper contextAwareGroupingFieldMapper = (ContextAwareGroupingFieldMapper) mapper;
                // Initial implementation is supported to 1 field, there are validations in mapper
                this.contextAwareGroupingFieldName = contextAwareGroupingFieldMapper.fieldType().fields().get(0);
                this.contextAwareGroupingScript = contextAwareGroupingFieldMapper.fieldType().compiledScript();
            }
        }
    }

    /**
     * Extracts context-aware criteria from a given query builder. This method analyzes different types
     * of queries to identify and extract relevant criteria values.
     *
     * <p>The method handles the following query types:
     * <ul>
     *   <li>WithFilterQueryBuilder - Processes the inner filter query</li>
     *   <li>TermQueryBuilder - Extracts criteria if field matches the context-aware grouping field</li>
     *   <li>TermsQueryBuilder - Extracts multiple criteria values if field matches the context-aware grouping field</li>
     *   <li>BoolQueryBuilder - Processes boolean combinations of queries</li>
     * </ul>
     *
     * @param queryBuilder The query builder to analyze for criteria extraction
     * @return A Set of String criteria values extracted from the query. Returns an empty set if:
     *         <ul>
     *           <li>The context-aware grouping field name is null</li>
     *           <li>The query builder is null</li>
     *           <li>The query type is not supported</li>
     *           <li>No matching criteria are found</li>
     *         </ul>
     */
    public Set<String> extractCriteria(QueryBuilder queryBuilder) {
        if (this.contextAwareGroupingFieldName == null || queryBuilder == null) {
            return Collections.emptySet();
        }

        switch (queryBuilder) {
            case WithFilterQueryBuilder withFilterQueryBuilder -> {
                return extractCriteria(withFilterQueryBuilder.filterQueryBuilder());
            }
            case TermsQueryBuilder termsQueryBuilder -> {
                if (termsQueryBuilder.fieldName().equals(contextAwareGroupingFieldName)) {
                    Set<String> criteria = new HashSet<>();
                    for (Object value : termsQueryBuilder.values()) {
                        criteria.add(executeScript(value.toString()));
                    }
                    return criteria;
                }
                return Collections.emptySet();
            }
            case TermQueryBuilder termQueryBuilder -> {
                if (termQueryBuilder.fieldName().equals(contextAwareGroupingFieldName)) {
                    return Set.of(executeScript(termQueryBuilder.value().toString()));
                }
                return Collections.emptySet();
            }
            case BoolQueryBuilder boolQueryBuilder -> {
                return extractCriteriaFromBooleanQuery(boolQueryBuilder);
            }
            default -> {
                return Collections.emptySet();
            }
        }
    }

    /**
     * Extracts criteria from a boolean query by analyzing its filter, must, and should clauses.
     * This method processes the clauses in order of precedence: filter -> must -> should.
     *
     * <p>The method implements the following logic and stopping conditions:
     * <ul>
     *   <li>Filter clauses:
     *     <ul>
     *       <li>Returns first filter's criteria if only one filter exists</li>
     *       <li>Returns empty set if multiple filters have different criteria</li>
     *     </ul>
     *   </li>
     *   <li>Must clauses (processed if no filter criteria):
     *     <ul>
     *       <li>Returns first must clause's criteria if only one exists</li>
     *       <li>Returns empty set if multiple must clauses have different criteria</li>
     *       <li>Continues accumulating if subsequent criteria match the first</li>
     *     </ul>
     *   </li>
     *   <li>Should clauses (processed if no must criteria and minimumShouldMatch > 0):
     *     <ul>
     *       <li>Returns empty set if any should clause has no criteria</li>
     *       <li>Accumulates all should clause criteria if all are valid</li>
     *     </ul>
     *   </li>
     * </ul>
     *
     * <p>Recursive Behavior:
     * <ul>
     *   <li>Calls {@code extractCriteria} recursively for each clause</li>
     *   <li>Recursion stops when a leaf query (non-boolean) is reached</li>
     *   <li>Early returns (stopping conditions) occur when:
     *     <ul>
     *       <li>Multiple filter clauses have different criteria</li>
     *       <li>Must clauses have conflicting criteria</li>
     *       <li>Any should clause returns empty criteria</li>
     *     </ul>
     *   </li>
     * </ul>
     *
     * @param boolQueryBuilder The boolean query to analyze
     * @return A Set of String criteria values, or an empty set if no valid criteria combination is found
     */
    private Set<String> extractCriteriaFromBooleanQuery(BoolQueryBuilder boolQueryBuilder) {
        List<QueryBuilder> filter = boolQueryBuilder.filter();
        Set<String> filterCriterias = new HashSet<>();
        for (QueryBuilder query : filter) {
            Set<String> criterias = extractCriteria(query);
            if (filterCriterias.isEmpty()) {
                filterCriterias.addAll(criterias);
            } else {
                return Collections.emptySet();
            }
        }

        if (!filterCriterias.isEmpty()) {
            return filterCriterias;
        }

        final Set<String> mustCriterias = new HashSet<>();
        List<QueryBuilder> mustClauses = boolQueryBuilder.must();
        for (QueryBuilder query : mustClauses) {
            Set<String> criterias = extractCriteria(query);
            if (mustCriterias.isEmpty()) {
                mustCriterias.addAll(criterias);
            } else {
                if (!(criterias.isEmpty() || mustCriterias.equals(criterias))) {
                    return Collections.emptySet();
                }
            }
        }

        if (!mustCriterias.isEmpty()) {
            return mustCriterias;
        }

        final Set<String> shouldCriterias = new HashSet<>();
        String minshouldMatchString = boolQueryBuilder.minimumShouldMatch();
        int minimumShouldMatch = minshouldMatchString == null ? 1 : Integer.parseInt(minshouldMatchString);
        if (mustCriterias.isEmpty() && minimumShouldMatch > 0) {
            final List<QueryBuilder> shouldClauses = boolQueryBuilder.should();
            for (QueryBuilder query : shouldClauses) {
                Set<String> shouldCriteria = extractCriteria(query);
                if (shouldCriteria.isEmpty()) {
                    return Collections.emptySet();
                }
                shouldCriterias.addAll(shouldCriteria);
            }
        }

        if (!shouldCriterias.isEmpty()) {
            return shouldCriterias;
        }

        return Collections.emptySet();
    }

    private String executeScript(final String groupingCriteriaValue) {
        if (contextAwareGroupingScript != null) {
            return contextAwareGroupingScript.execute(Map.of(contextAwareGroupingFieldName, fieldValue(groupingCriteriaValue)));
        }
        return groupingCriteriaValue;
    }

    private Object fieldValue(String groupingCriteriaValue) {
        Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(this.contextAwareGroupingFieldName);
        switch (fieldMapper) {
            case NumberFieldMapper mapper -> {
                return mapper.fieldType().parse(groupingCriteriaValue);
            }
            default -> {
                return groupingCriteriaValue;
            }
        }
    }
}
