/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class MandatoryQueryConstraintExtractorTests extends OpenSearchTestCase {
    private final MandatoryQueryConstraintExtractor extractor = new MandatoryQueryConstraintExtractor();

    public void testExtractsConfiguredRangeFromMandatoryQueryBranches() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("service.name", "api"))
                .must(QueryBuilders.rangeQuery("@timestamp").gte(100L).lt(200L))
                .should(QueryBuilders.rangeQuery("ignored").gte(1L))
        );

        List<QueryConstraint> constraints = extractor.extractMandatoryConstraints(source, Set.of("@timestamp"));

        assertThat(constraints.size(), equalTo(1));
        assertThat(constraints.get(0), instanceOf(RangeQueryConstraint.class));

        RangeQueryConstraint constraint = (RangeQueryConstraint) constraints.get(0);
        assertThat(constraint.field(), equalTo("@timestamp"));
        assertThat(constraint.lowerValue(), equalTo(100L));
        assertThat(constraint.upperValue(), equalTo(200L));
        assertTrue(constraint.includeLower());
        assertFalse(constraint.includeUpper());
    }

    public void testExtractsRangeFromConstantScoreFilter() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.constantScoreQuery(QueryBuilders.rangeQuery("@timestamp").from(100L).to(200L))
        );

        List<QueryConstraint> constraints = extractor.extractMandatoryConstraints(source, Set.of("@timestamp"));

        assertThat(constraints.size(), equalTo(1));
        assertThat(constraints.get(0).field(), equalTo("@timestamp"));
    }

    public void testExtractsDateMathRangeAsRawValues() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("@timestamp").gte("now-2m").lte("now"));

        List<QueryConstraint> constraints = extractor.extractMandatoryConstraints(source, Set.of("@timestamp"));

        assertThat(constraints.size(), equalTo(1));
        assertThat(constraints.get(0), instanceOf(RangeQueryConstraint.class));

        RangeQueryConstraint constraint = (RangeQueryConstraint) constraints.get(0);
        assertThat(constraint.field(), equalTo("@timestamp"));
        assertThat(constraint.lowerValue(), equalTo("now-2m"));
        assertThat(constraint.upperValue(), equalTo("now"));
        assertTrue(constraint.includeLower());
        assertTrue(constraint.includeUpper());
    }

    public void testDoesNotExtractRangesFromOptionalBranches() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("@timestamp").gte(100L).lte(200L))
        );

        assertTrue(extractor.extractMandatoryConstraints(source, Set.of("@timestamp")).isEmpty());
    }

    public void testDoesNotExtractRangesFromDisMaxBranches() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.disMaxQuery()
                .add(QueryBuilders.rangeQuery("@timestamp").gte(100L).lte(200L))
                .add(QueryBuilders.termQuery("service.name", "api"))
        );

        assertTrue(extractor.extractMandatoryConstraints(source, Set.of("@timestamp")).isEmpty());
    }

    public void testDoesNotExtractRangesFromNegativeBranches() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.rangeQuery("@timestamp").gte(100L).lte(200L))
        );

        assertTrue(extractor.extractMandatoryConstraints(source, Set.of("@timestamp")).isEmpty());
    }

    public void testExtractsRangeQueryInterpretationOptions() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.rangeQuery("@timestamp")
                .gte("2024-01-01")
                .format("strict_date_optional_time")
                .timeZone("+01:00")
                .relation("within")
        );

        List<QueryConstraint> constraints = extractor.extractMandatoryConstraints(source, Set.of("@timestamp"));

        assertThat(constraints.size(), equalTo(1));
        assertThat(constraints.get(0), instanceOf(RangeQueryConstraint.class));

        RangeQueryConstraint constraint = (RangeQueryConstraint) constraints.get(0);
        assertThat(constraint.format(), equalTo("strict_date_optional_time"));
        assertThat(constraint.timeZone(), equalTo("+01:00"));
        assertThat(constraint.relation(), equalTo(ShapeRelation.WITHIN));
    }

    public void testIgnoresUnconfiguredFields() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("@timestamp").gte(100L).lte(200L));

        assertTrue(extractor.extractMandatoryConstraints(source, Set.of("event.ingested")).isEmpty());
    }
}
