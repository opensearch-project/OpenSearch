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

        QueryPlans.QueryPlan plan = plans.get(QueryPlans.Type.HITS).get(0);
        assertTrue(plan.relNode() instanceof LogicalTableScan);
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
