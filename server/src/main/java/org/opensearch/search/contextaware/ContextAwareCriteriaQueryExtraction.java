/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.contextaware;

import org.opensearch.index.mapper.ContextAwareGroupingFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WithFilterQueryBuilder;
import org.opensearch.script.ContextAwareGroupingScript;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ContextAwareCriteriaQueryExtraction
 */
public class ContextAwareCriteriaQueryExtraction {

    private final MapperService mapperService;
    private ContextAwareGroupingScript contextAwareGroupingScript;
    private String contextAwareGroupingFieldName;

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

    public Set<String> extractCriteria(QueryBuilder queryBuilder) {
        if (this.contextAwareGroupingFieldName == null || queryBuilder == null) {
            return Collections.emptySet();
        }

        switch (queryBuilder) {
            case WithFilterQueryBuilder withFilterQueryBuilder -> {
                return extractCriteria(withFilterQueryBuilder.filterQueryBuilder());
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
