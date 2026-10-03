/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchSourceConverterTests extends OpenSearchTestCase {

    private SearchSourceConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();
        schema.add("test-index", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                // Nullable fields — matches OpenSearchSchemaBuilder behavior
                return typeFactory.builder()
                    .add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("price", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("brand", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true))
                    .add("rating", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true))
                    .build();
            }
        });
        converter = new SearchSourceConverter(schema);
    }

    public void testConvertProducesHitsPlan() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        // hits plan plus the COUNT plan that supplies hits.total
        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.COUNT));

        // Even a default request carries fetch=size (10) so the limit pushes down to the engine.
        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertTrue(plan.relNode() instanceof LogicalSort);
        LogicalSort sort = (LogicalSort) plan.relNode();
        assertNotNull(sort.fetch);
        assertTrue(sort.getInput() instanceof LogicalTableScan);
    }

    public void testConvertResolvesFieldNames() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertEquals(4, plan.relNode().getRowType().getFieldCount());
        assertEquals(List.of("name", "price", "brand", "rating"), plan.relNode().getRowType().getFieldNames());
    }

    public void testConvertThrowsForMissingIndex() {
        expectThrows(IllegalArgumentException.class, () -> converter.convert(new SearchSourceBuilder(), "nonexistent-index"));
    }

    public void testAggsWithSizeZeroProducesAggregationAndCountPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(2, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testAggsWithSizeGreaterThanZeroProducesAllPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(10).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(3, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testNoAggsProducesHitsAndCountPlans() throws ConversionException {
        QueryPlans plans = converter.convert(new SearchSourceBuilder(), "test-index");

        assertEquals(2, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testSizeZeroNoAggsProducesOnlyCountPlan() throws ConversionException {
        // size=0 with no aggs is the count-only query: hits.total comes from the COUNT plan
        SearchSourceBuilder source = new SearchSourceBuilder().size(0);
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(1, plans.getAll().size());
        assertFalse(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.AGGREGATION));
        assertTrue(plans.has(QueryPlans.Type.COUNT));
    }

    public void testTrackTotalHitsDisabledSkipsCountPlan() throws ConversionException {
        // no eligible counts needed and totals explicitly not tracked → nothing to count
        SearchSourceBuilder source = new SearchSourceBuilder().trackTotalHits(false);
        QueryPlans plans = converter.convert(source, "test-index");

        assertEquals(1, plans.getAll().size());
        assertTrue(plans.has(QueryPlans.Type.HITS));
        assertFalse(plans.has(QueryPlans.Type.COUNT));
    }

    public void testAggPlanIncludesPostAggSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .order(BucketOrder.key(true))
                    .subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
            );
        QueryPlans plans = converter.convert(source, "test-index");

        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        // Aggregation plan should be wrapped with LogicalSort for bucket order
        assertTrue(plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode() instanceof LogicalSort);
    }

    public void testMetricOnlyAggPlanHasNoPostAggSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new AvgAggregationBuilder("avg_price").field("price"));
        QueryPlans plans = converter.convert(source, "test-index");

        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));
        // Metric-only agg has no bucket orders, so no LogicalSort wrapper
        assertFalse(plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode() instanceof LogicalSort);
    }

    // ---- Top-K pushdown plan shapes ----

    public void testTermsSizeBecomesFetchOnAggregationSort() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").size(7));
        QueryPlans plans = converter.convert(source, "test-index");

        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("sort must carry the fetch: " + plan, plan.contains("fetch=[7]"));
    }

    public void testTermsDefaultNullExclusionBecomesPreAggFilter() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"));
        QueryPlans plans = converter.convert(source, "test-index");

        // brand is column 2 in the test schema
        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("null keys must be excluded below the aggregate: " + plan, plan.contains("LogicalFilter(condition=[IS NOT NULL($2)])"));
    }

    public void testMinDocCountBecomesHavingWithFetch() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5));
        QueryPlans plans = converter.convert(source, "test-index");

        // post-agg schema is [brand, _count] — the HAVING filter references _count at $1,
        // and the LIMIT rides the sort above it (filter-before-truncate)
        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("min_doc_count must become a HAVING filter: " + plan, plan.contains(">=($1, 5)"));
        assertTrue("the plan stays bounded: " + plan, plan.contains("fetch=[10]"));
    }

    public void testMinDocCountGetsOwnEligibleCountPlan() throws ConversionException {
        // The eligible count must exclude below-threshold groups, which COUNT(field)
        // cannot see — the aggregation gets its own HAVING-filtered SUM plan.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5));
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertEquals(2, countPlans.size());
        // flat plan: hits.total only — the eligible count moved to the dedicated plan
        QueryPlans.QueryPlan flat = countPlanWithColumn(countPlans, QueryPlans.COUNT_TOTAL_COLUMN);
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN), flat.relNode().getRowType().getFieldNames());
        // dedicated plan: SUM over the HAVING-filtered per-group counts
        QueryPlans.QueryPlan eligibleCountPlan = countPlanWithColumn(countPlans, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand");
        assertEquals(
            List.of(QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand"),
            eligibleCountPlan.relNode().getRowType().getFieldNames()
        );
        String plan = eligibleCountPlan.relNode().explain();
        assertTrue("eligible count must be HAVING-filtered: " + plan, plan.contains(">=($1, 5)"));
        assertTrue("eligible count must sum the surviving group counts: " + plan, plan.contains("SUM($1)"));
    }

    public void testMissingWithMinDocCountGetsOwnEligibleCountPlan() throws ConversionException {
        // missing + min_doc_count together: substitution makes every matching doc form a group,
        // but the threshold still drops whole groups from eligibility — the eligible count must be
        // the HAVING-filtered SUM, not COUNT(*) (which would count dropped groups' docs).
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").minDocCount(5).missing("N/A"));
        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertEquals(2, countPlans.size());
        // flat plan: hits.total only — the eligible count must not ride COUNT(*)
        QueryPlans.QueryPlan flat = countPlanWithColumn(countPlans, QueryPlans.COUNT_TOTAL_COLUMN);
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN), flat.relNode().getRowType().getFieldNames());
        QueryPlans.QueryPlan eligibleCountPlan = countPlanWithColumn(countPlans, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand");
        assertEquals(
            List.of(QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand"),
            eligibleCountPlan.relNode().getRowType().getFieldNames()
        );
        String plan = eligibleCountPlan.relNode().explain();
        assertTrue("eligible count must be HAVING-filtered: " + plan, plan.contains(">=($1, 5)"));
        assertTrue("eligible count must carry the missing substitution: " + plan, plan.contains("CASE(IS NOT NULL($2)"));
        assertTrue("eligible count must sum the surviving group counts: " + plan, plan.contains("SUM($1)"));
    }

    /** Finds the COUNT plan carrying the given output column — plan order is not part of the contract. */
    private static QueryPlans.QueryPlan countPlanWithColumn(List<QueryPlans.QueryPlan> countPlans, String column) {
        return countPlans.stream()
            .filter(p -> p.relNode().getRowType().getFieldNames().contains(column))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no COUNT plan with column [" + column + "]"));
    }

    public void testSameFieldSiblingsProduceTwoBoundedPlans() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("top_brands").field("brand"))
            .aggregation(new TermsAggregationBuilder("brands_by_key").field("brand").order(BucketOrder.key(true)));
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());
        for (QueryPlans.QueryPlan plan : aggPlans) {
            assertTrue("each sibling's plan is bounded: " + plan.relNode().explain(), plan.relNode().explain().contains("fetch=[10]"));
        }
        // one eligible-count column per sibling, keyed by aggregation name
        List<String> countColumns = plans.get(QueryPlans.Type.COUNT).get(0).relNode().getRowType().getFieldNames();
        assertEquals(
            List.of(
                QueryPlans.COUNT_TOTAL_COLUMN,
                QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "top_brands",
                QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "brands_by_key"
            ),
            countColumns
        );
    }

    public void testMissingBecomesCaseProjectionWithoutNullFilter() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").missing("unknown"));
        QueryPlans plans = converter.convert(source, "test-index");

        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("missing must substitute via CASE: " + plan, plan.contains("CASE(IS NOT NULL($2)"));
        assertFalse("missing fields keep their null keys: " + plan, plan.contains("LogicalFilter(condition=[IS NOT NULL($2)])"));
    }

    public void testCountPlanCarriesTotalAndEligibleColumns() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(new TermsAggregationBuilder("by_brand").field("brand"));
        QueryPlans plans = converter.convert(source, "test-index");

        List<String> columns = plans.get(QueryPlans.Type.COUNT).get(0).relNode().getRowType().getFieldNames();
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand"), columns);
    }

    public void testCountPlanWithMissingCarriesOnlyTotal() throws ConversionException {
        // missing substitution makes every matching doc eligible — COUNT(*) is the eligible count
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_brand").field("brand").missing("unknown"));
        QueryPlans plans = converter.convert(source, "test-index");

        List<String> columns = plans.get(QueryPlans.Type.COUNT).get(0).relNode().getRowType().getFieldNames();
        assertEquals(List.of(QueryPlans.COUNT_TOTAL_COLUMN), columns);
    }

    public void testNestedTermsPlanIsBoundedPerParent() throws ConversionException {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new TermsAggregationBuilder("by_brand").field("brand")
                    .size(2)
                    .subAggregation(new TermsAggregationBuilder("by_name").field("name").size(3))
            );
        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());

        String parentPlan = aggPlans.get(0).relNode().explain();
        assertTrue("parent is a flat top-N: " + parentPlan, parentPlan.contains("fetch=[2]"));

        QueryPlans.QueryPlan child = aggPlans.get(1);
        String childPlan = child.relNode().explain();
        // restricted to the parent plan's winners
        assertTrue("child must semi-join the parent plan: " + childPlan, childPlan.contains("joinType=[semi]"));
        // bounded per parent, ordered by the child's bucket order inside the window
        assertTrue("child must rank within the parent partition: " + childPlan, childPlan.contains("ROW_NUMBER() OVER (PARTITION BY"));
        assertTrue("rank filter must keep the per-parent top K: " + childPlan, childPlan.contains("<=($"));
        // the eligible count rides the rows
        assertTrue("per-parent eligible total must ride the rows: " + childPlan, childPlan.contains("SUM($"));
        assertTrue(
            "child schema carries the parent-eligible column",
            child.relNode().getRowType().getFieldNames().contains("_parent_eligible")
        );
        // The child's own bound is the window rank, not a flat LIMIT: its size (3) must never
        // appear as a fetch. (The parent's fetch=[2] legitimately appears inside the child
        // plan — the parent top-N subtree is the semi-join input.)
        assertFalse("child size must not be a flat fetch: " + childPlan, childPlan.contains("fetch=[3]"));
        assertNull(child.aggregationMetadata().getFetch());
    }

    public void testUnsupportedTermsParameterRejectedEndToEnd() {
        TermsAggregationBuilder terms = new TermsAggregationBuilder("by_brand").field("brand");
        terms.includeExclude(new org.opensearch.search.aggregations.bucket.terms.IncludeExclude("Brand.*", null));
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(terms);

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertTrue(e.getMessage().contains("include"));
    }

    // ---- Golden file driven RelNode generation tests ----

    /**
     * Auto-discovers all golden JSON files and validates that each inputDsl
     * produces the expected RelNode plan via SearchSourceConverter.convert().
     * Adding a new test case only requires adding a new JSON file — no new
     * Java method needed.
     */
    public void testGoldenFileRelNodeGeneration() throws Exception {
        URL goldenDir = getClass().getClassLoader().getResource("golden");
        assertNotNull("Golden file resource directory not found", goldenDir);

        List<Path> goldenFiles;
        try (var stream = Files.list(Path.of(goldenDir.toURI()))) {
            goldenFiles = stream.filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList());
        }
        assertFalse("No golden files found", goldenFiles.isEmpty());

        List<String> failures = new ArrayList<>();
        for (Path file : goldenFiles) {
            String fileName = file.getFileName().toString();
            try {
                GoldenTestCase tc = GoldenFileLoader.load(fileName);
                CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

                SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
                SearchSourceConverter conv = new SearchSourceConverter(infra.schema());
                QueryPlans plans = conv.convert(searchSource, tc.getIndexName());

                QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
                List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
                if (matchingPlans.isEmpty()) {
                    failures.add(fileName + ": No " + expectedType + " plan produced");
                    continue;
                }

                RelNode relNode = matchingPlans.get(0).relNode();
                String actualPlan = relNode.explain().trim();
                String expectedPlan = String.join("\n", tc.getExpectedRelNodePlan());

                if (!expectedPlan.equals(actualPlan)) {
                    failures.add(fileName + ": RelNode plan mismatch\n  Expected: " + expectedPlan + "\n  Actual:   " + actualPlan);
                }

                List<String> actualFields = relNode.getRowType().getFieldNames();
                if (!tc.getMockResultFieldNames().equals(actualFields)) {
                    failures.add(
                        fileName + ": Field names mismatch\n  Expected: " + tc.getMockResultFieldNames() + "\n  Actual:   " + actualFields
                    );
                }
            } catch (Exception e) {
                failures.add(fileName + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            fail("Golden file RelNode generation failures:\n" + String.join("\n", failures));
        }
    }

    public void testNanoTimestampTypeSystemReportsMaxPrecisionNine() {
        assertEquals(9, DslTypeSystems.NANO_TIMESTAMP.getMaxPrecision(SqlTypeName.TIMESTAMP));
    }

    // ---- Filter aggregation tests ----

    public void testFilterAggUntranslatableQueryRejects() {
        // A filter aggregation with an unregistered query type must reject at conversion time.
        // wildcard has no registered translator, so it remains an UnresolvedQueryCall and rejects.
        org.opensearch.index.query.WildcardQueryBuilder unsupportedQuery = new org.opensearch.index.query.WildcardQueryBuilder(
            "name",
            "lap*"
        );
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder("my_filter", unsupportedQuery));

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertTrue(e.getMessage().contains("unsupported query type"));
    }

    public void testFilterAggPlanShapeSingleScan() throws ConversionException {
        // A filter agg with a supported query produces a plan with LogicalFilter below LogicalAggregate
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "active_only",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                )
            );

        QueryPlans plans = converter.convert(source, "test-index");
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));

        RelNode aggPlan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode();
        String plan = aggPlan.explain();
        // Must have exactly one LogicalTableScan
        assertEquals(1, plan.split("LogicalTableScan").length - 1);
        // Must have a LogicalFilter with the term predicate below the aggregate
        assertTrue("plan must contain LogicalFilter: " + plan, plan.contains("LogicalFilter"));
        assertTrue("plan must contain LogicalAggregate: " + plan, plan.contains("LogicalAggregate"));
    }

    public void testFilterAggBoolQueryAccepted() throws ConversionException {
        // A compound bool query (must + must_not) is translatable via the registered
        // BoolQueryTranslator, so a filter aggregation using one converts successfully and
        // produces a LogicalFilter below the LogicalAggregate.
        org.opensearch.index.query.BoolQueryBuilder boolQuery = new org.opensearch.index.query.BoolQueryBuilder().must(
            new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
        ).mustNot(new org.opensearch.index.query.TermQueryBuilder("name", "Widget"));
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder("compound_filter", boolQuery));

        QueryPlans plans = converter.convert(source, "test-index");
        assertTrue(plans.has(QueryPlans.Type.AGGREGATION));

        String plan = plans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        assertTrue("plan must contain LogicalFilter: " + plan, plan.contains("LogicalFilter"));
        assertTrue("plan must contain LogicalAggregate: " + plan, plan.contains("LogicalAggregate"));
    }

    // ---- Ancestor filter propagation tests ----
    // These verify that a filter aggregation's predicate propagates to nested bucket
    // sub-aggregations — not just metric children.

    public void testFilterWithNestedTermsChildPropagatesFilter() throws ConversionException {
        // A filter(term: brand=BrandA) with a nested terms(field: name) child must produce a
        // child plan whose input carries the filter predicate. Without propagation, the child
        // would aggregate over the full corpus instead of only filtered documents.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new TermsAggregationBuilder("by_name").field("name"))
            );

        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals("filter + nested terms must produce 2 plans", 2, aggPlans.size());

        // The child plan (by_name) must contain the ancestor filter predicate
        String childPlan = aggPlans.get(1).relNode().explain();
        assertTrue(
            "child terms plan must carry the ancestor filter predicate: " + childPlan,
            childPlan.contains("$2") && childPlan.contains("BrandA")
        );
    }

    public void testFilterWithSizedTermsChildPropagatesFilterWithTruncation() throws ConversionException {
        // A filter(term: brand=BrandA) with terms(field: name, size: 2) — the bounded child
        // must both carry the ancestor filter AND apply its own top-K truncation.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new TermsAggregationBuilder("by_name").field("name").size(2))
            );

        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals("filter + nested sized terms must produce 2 plans", 2, aggPlans.size());

        String childPlan = aggPlans.get(1).relNode().explain();
        assertTrue("child plan must carry the ancestor filter: " + childPlan, childPlan.contains("$2") && childPlan.contains("BrandA"));
        assertTrue("child plan must carry its own top-K truncation: " + childPlan, childPlan.contains("fetch=[2]"));
    }

    public void testNestedFilterInsideFilterConjoinsPredicates() throws ConversionException {
        // filter(term: brand=BrandA) → filter(term: name=Widget) → terms(field: rating)
        // The leaf terms plan must carry BOTH ancestor predicates conjoined.
        // Uses fields in the test schema: brand(idx 2), name(idx 0), rating(idx 3)
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(
                    new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                        "name_filter",
                        new org.opensearch.index.query.TermQueryBuilder("name", "Widget")
                    ).subAggregation(new TermsAggregationBuilder("by_rating").field("rating"))
                )
            );

        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals("outer filter + inner filter + leaf terms must produce 3 plans", 3, aggPlans.size());

        // The leaf plan (by_rating) must carry both predicates
        String leafPlan = aggPlans.get(2).relNode().explain();
        assertTrue("leaf plan must carry the outer filter (brand=BrandA): " + leafPlan, leafPlan.contains("BrandA"));
        assertTrue("leaf plan must carry the inner filter (name=Widget): " + leafPlan, leafPlan.contains("Widget"));
    }

    public void testFilterWithBothMetricAndBucketChildPropagates() throws ConversionException {
        // filter(term: brand=BrandA) with BOTH avg(price) and terms(name) children.
        // Both plans must carry the filter.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new AvgAggregationBuilder("avg_price").field("price"))
                    .subAggregation(new TermsAggregationBuilder("by_name").field("name"))
            );

        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals("filter with metric+bucket children produces 2 plans", 2, aggPlans.size());

        // The filter's own plan (brand_filter) carries the metric and must have the filter
        String parentPlan = aggPlans.get(0).relNode().explain();
        assertTrue(
            "parent plan (metric child rides here) must carry the filter: " + parentPlan,
            parentPlan.contains("$2") && parentPlan.contains("BrandA")
        );

        // The child bucket plan (by_name) must also carry the ancestor filter
        String childPlan = aggPlans.get(1).relNode().explain();
        assertTrue(
            "child bucket plan must carry the ancestor filter: " + childPlan,
            childPlan.contains("$2") && childPlan.contains("BrandA")
        );
    }

    public void testUntranslatableAncestorFilterNamesOffendingAggregation() {
        // An untranslatable filter in an ANCESTOR must report that ancestor's aggregation name,
        // not the child's, so operators can locate the unsupported query.
        org.opensearch.index.query.WildcardQueryBuilder unsupportedQuery = new org.opensearch.index.query.WildcardQueryBuilder(
            "brand",
            "Brand*"
        );
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder("bad_ancestor", unsupportedQuery)
                    .subAggregation(new TermsAggregationBuilder("by_name").field("name"))
            );

        ConversionException e = expectThrows(ConversionException.class, () -> converter.convert(source, "test-index"));
        assertTrue("error must name the ancestor aggregation 'bad_ancestor': " + e.getMessage(), e.getMessage().contains("bad_ancestor"));
        assertTrue("error must mention 'unsupported query type': " + e.getMessage(), e.getMessage().contains("unsupported query type"));
    }

    public void testFilterMatchAllWithTermsChildNoSpuriousFilter() throws ConversionException {
        // filter(match_all) with terms(name): the child must NOT have a spurious filter injected.
        // match_all translates to a literal TRUE which Calcite simplifies away, so the plan
        // must look identical to a bare terms(name) without any enclosing filter.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "all_filter",
                    new org.opensearch.index.query.MatchAllQueryBuilder()
                ).subAggregation(new TermsAggregationBuilder("by_name").field("name"))
            );

        // Bare terms(name) for comparison
        SearchSourceBuilder bareSource = new SearchSourceBuilder().size(0)
            .aggregation(new TermsAggregationBuilder("by_name").field("name"));

        QueryPlans plansWithFilter = converter.convert(source, "test-index");
        QueryPlans barePlans = converter.convert(bareSource, "test-index");

        List<QueryPlans.QueryPlan> filteredAggPlans = plansWithFilter.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, filteredAggPlans.size());

        // The child plan under match_all filter should have the same null-exclusion filter
        // as a bare terms — no additional predicate injected by the ancestor.
        String childPlan = filteredAggPlans.get(1).relNode().explain();
        String barePlan = barePlans.get(QueryPlans.Type.AGGREGATION).get(0).relNode().explain();
        // Both must have IS NOT NULL for name field (column 0) - standard terms behavior
        assertTrue("child must null-filter: " + childPlan, childPlan.contains("IS NOT NULL($0)"));
        assertTrue("bare must null-filter: " + barePlan, barePlan.contains("IS NOT NULL($0)"));
        // The child must NOT have any extra filter beyond the null-exclusion
        assertEquals(
            "match_all ancestor must not inject a spurious predicate — filter count must match bare terms",
            barePlan.split("LogicalFilter").length,
            childPlan.split("LogicalFilter").length
        );
    }

    // ---- Eligible-doc count (sum_other_doc_count) scoping under filter parents ----

    public void testFilterWithSizedTermsChildEligibleCountCarriesAncestorFilter() throws ConversionException {
        // A filter(term: brand=BrandA) with terms(field: name, size: 2): the eligible-doc count
        // plan for by_name must be filtered to only documents matching brand=BrandA. Without this,
        // sum_other_doc_count = eligibleDocCount - returnedDocCount would use the full corpus
        // count instead of the filtered count, yielding a value that exceeds the parent's
        // doc_count (the defect observed in live cluster testing: 8 - 3 = 5, when the filtered
        // parent only has 3 eligible docs).
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new TermsAggregationBuilder("by_name").field("name").size(2))
            );

        QueryPlans plans = converter.convert(source, "test-index");

        // The child terms is bounded (size=2) so it needs an eligible-doc count plan.
        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertFalse("must have at least one COUNT plan", countPlans.isEmpty());

        // Find the count plan carrying the eligible column for "by_name"
        String eligibleColumn = QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_name";
        QueryPlans.QueryPlan eligiblePlan = countPlans.stream()
            .filter(p -> p.relNode().getRowType().getFieldNames().contains(eligibleColumn))
            .findFirst()
            .orElse(null);
        assertNotNull("must have an eligible-count plan for by_name", eligiblePlan);

        // The eligible-count plan MUST include the ancestor filter predicate (brand=BrandA)
        // so that sum_other_doc_count is scoped to the filtered document set.
        String plan = eligiblePlan.relNode().explain();
        assertTrue("eligible-count plan must carry the ancestor filter predicate (brand=BrandA): " + plan, plan.contains("BrandA"));
    }

    public void testFilterWithMissingTermsChildEligibleCountCarriesAncestorFilter() throws ConversionException {
        // Invariant: a filter parent must scope its bounded child's eligible-doc count to the
        // FILTERED document set even when the child substitutes `missing`. The substitution must
        // not divert the eligible count onto the unfiltered COUNT(*); otherwise
        // sum_other_doc_count = corpusCount - returned exceeds the parent's doc_count.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new TermsAggregationBuilder("by_name").field("name").missing("N/A").size(1))
            );

        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertFalse("must have at least one COUNT plan", countPlans.isEmpty());

        String eligibleColumn = QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_name";
        QueryPlans.QueryPlan eligiblePlan = countPlans.stream()
            .filter(p -> p.relNode().getRowType().getFieldNames().contains(eligibleColumn))
            .findFirst()
            .orElse(null);
        assertNotNull(
            "a missing-configured bounded child must still get its own filtered eligible-count plan, " + "not ride the unfiltered COUNT(*)",
            eligiblePlan
        );

        String plan = eligiblePlan.relNode().explain();
        assertTrue(
            "eligible-count plan for a missing-configured child must carry the ancestor filter predicate (brand=BrandA): " + plan,
            plan.contains("BrandA")
        );
    }

    public void testFilterWithMissingTermsChildEligibleCountCountsAllFilteredDocs() throws ConversionException {
        // Invariant: when `missing` substitutes null keys into a bucket, null-field docs are
        // eligible, so the filtered eligible count must count ALL filtered docs (COUNT() over an
        // empty argument list) rather than only non-null group values (COUNT(field)).
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "brand_filter",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new TermsAggregationBuilder("by_name").field("name").missing("N/A").size(1))
            );

        QueryPlans plans = converter.convert(source, "test-index");

        List<QueryPlans.QueryPlan> countPlans = plans.get(QueryPlans.Type.COUNT);
        assertFalse("must have at least one COUNT plan", countPlans.isEmpty());

        String eligibleColumn = QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_name";
        QueryPlans.QueryPlan eligiblePlan = countPlans.stream()
            .filter(p -> p.relNode().getRowType().getFieldNames().contains(eligibleColumn))
            .findFirst()
            .orElse(null);
        assertNotNull("a missing-configured bounded child must have its own filtered eligible-count plan", eligiblePlan);

        String plan = eligiblePlan.relNode().explain();
        assertTrue(
            "eligible count for a missing-configured child must count ALL filtered rows (COUNT()), "
                + "not restrict to non-null group values (COUNT(field)): "
                + plan,
            plan.contains(eligibleColumn + "=[COUNT()]")
        );
    }

    public void testDescendantFilterReusingAncestorNameKeepsAncestorPredicate() throws ConversionException {
        // Invariant: aggregation names are unique only among siblings, so a descendant may
        // legally reuse an ancestor's name. The by-name self-deduplication must not confuse a
        // same-named ancestor filter with the defining aggregation's own filter — the innermost
        // plan must carry BOTH the outer and inner predicates.
        // Shape: filter "dup"(brand=BrandA) -> terms "grp"(rating) -> filter "dup"(name=Widget)
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "dup",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(
                    new TermsAggregationBuilder("grp").field("rating")
                        .subAggregation(
                            new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                                "dup",
                                new org.opensearch.index.query.TermQueryBuilder("name", "Widget")
                            )
                        )
                )
            );

        QueryPlans plans = converter.convert(source, "test-index");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals("outer filter + terms + inner filter must produce 3 plans", 3, aggPlans.size());

        // Innermost plan is the re-named inner filter "dup" (walker emits parents before children).
        String innermostPlan = aggPlans.get(2).relNode().explain();
        assertTrue("innermost plan must carry its own filter (name=Widget): " + innermostPlan, innermostPlan.contains("Widget"));
        assertTrue(
            "innermost plan must ALSO carry the same-named ancestor filter (brand=BrandA): " + innermostPlan,
            innermostPlan.contains("BrandA")
        );
    }

    // ---- Range inner query as a filter aggregation (GAP A) ----

    public void testFilterRangeInnerQueryPredicateReachesPlan() throws ConversionException {
        // Invariant: a range inner query on a filter aggregation must translate to a bounded
        // predicate carrying BOTH range endpoints — a dropped or mistranslated bound must fail.
        // Shape: filter "price_band"(range price gte 300 lt 600) -> terms "by_brand"(brand).
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "price_band",
                    new org.opensearch.index.query.RangeQueryBuilder("price").gte(300).lt(600)
                ).subAggregation(new TermsAggregationBuilder("by_brand").field("brand"))
            );

        QueryPlans plans = converter.convert(source, "test-index");
        String bandPlan = aggPlanEndingWith(plans, "price_band").relNode().explain();
        // price is column 1 in the test schema; both bounds must be present and correctly oriented
        assertTrue("range plan must carry the lower bound gte 300: " + bandPlan, bandPlan.contains(">=($1, CAST(300):INTEGER)"));
        assertTrue("range plan must carry the upper bound lt 600: " + bandPlan, bandPlan.contains("<($1, CAST(600):INTEGER)"));
    }

    // ---- Falsifiable ancestor-predicate propagation (GAP B) ----
    // A filter aggregation's predicate is injected into its bucket descendants' plans by
    // SearchSourceConverter.applyAggregationFilters. A silently dropped injection would leave a
    // descendant aggregating the full corpus; these tests assert the SPECIFIC predicate literal
    // reaches the SPECIFIC descendant plan, so such a drop fails.

    public void testNestedFilterPredicateReachesChildBucketPlan() throws ConversionException {
        // A filter aggregation must scope its bucket sub-aggregation: the filter's predicate must
        // reach the nested child's plan, otherwise the child aggregates the full corpus. Shape:
        // filter "only_active"(term name=Widget) -> terms "by_rating"(rating). The child plan must
        // carry the ancestor filter's predicate (name=Widget, column 0).
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "only_active",
                    new org.opensearch.index.query.TermQueryBuilder("name", "Widget")
                ).subAggregation(new TermsAggregationBuilder("by_rating").field("rating"))
            );

        QueryPlans plans = converter.convert(source, "test-index");
        String childPlan = aggPlanEndingWith(plans, "by_rating").relNode().explain();
        assertTrue(
            "nested child plan must carry the filter's predicate (name=Widget): " + childPlan,
            childPlan.contains("$0") && childPlan.contains("Widget")
        );
    }

    public void testSiblingFilterPredicatesReachOwnChildPlansNoCrossContamination() throws ConversionException {
        // Two sibling filters, each with its own terms child, must each scope ONLY their own
        // child — no cross-contamination between siblings. Shapes:
        // f_widget(term name=Widget) -> terms "cat_a"(brand)
        // f_brand (term brand=BrandA) -> terms "cat_b"(rating)
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "f_widget",
                    new org.opensearch.index.query.TermQueryBuilder("name", "Widget")
                ).subAggregation(new TermsAggregationBuilder("cat_a").field("brand"))
            )
            .aggregation(
                new org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder(
                    "f_brand",
                    new org.opensearch.index.query.TermQueryBuilder("brand", "BrandA")
                ).subAggregation(new TermsAggregationBuilder("cat_b").field("rating"))
            );

        QueryPlans plans = converter.convert(source, "test-index");
        String catA = aggPlanEndingWith(plans, "cat_a").relNode().explain();
        String catB = aggPlanEndingWith(plans, "cat_b").relNode().explain();

        // Each child carries its OWN parent filter's predicate...
        assertTrue("cat_a must carry its own parent's predicate (name=Widget): " + catA, catA.contains("Widget"));
        assertTrue("cat_b must carry its own parent's predicate (brand=BrandA): " + catB, catB.contains("BrandA"));
        // ...and NOT the sibling's predicate.
        assertFalse("cat_a must not carry the sibling's predicate (BrandA): " + catA, catA.contains("BrandA"));
        assertFalse("cat_b must not carry the sibling's predicate (Widget): " + catB, catB.contains("Widget"));
    }

    /** Returns the single AGGREGATION plan whose aggregation-name path ends with the given name. */
    private static QueryPlans.QueryPlan aggPlanEndingWith(QueryPlans plans, String lastAggName) {
        return plans.get(QueryPlans.Type.AGGREGATION).stream().filter(p -> {
            List<String> path = p.aggregationMetadata().getAggNamePath();
            return path.get(path.size() - 1).equals(lastAggName);
        })
            .reduce((a, b) -> { throw new AssertionError("multiple plans end with [" + lastAggName + "]"); })
            .orElseThrow(() -> new AssertionError("no AGGREGATION plan ends with [" + lastAggName + "]"));
    }

    private SearchSourceBuilder parseSearchSource(Map<String, Object> inputDsl) throws IOException {
        String json;
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(inputDsl);
            json = builder.toString();
        }
        NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents()
        );
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(registry, DeprecationHandler.IGNORE_DEPRECATIONS, json)) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }
}
