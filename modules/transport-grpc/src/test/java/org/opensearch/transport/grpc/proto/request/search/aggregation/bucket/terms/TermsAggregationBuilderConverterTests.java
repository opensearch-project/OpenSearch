/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;

import org.opensearch.protobufs.AggregationContainer;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.protobufs.SortOrderSingleMap;
import org.opensearch.protobufs.StringArray;
import org.opensearch.protobufs.TermsAggregation;
import org.opensearch.protobufs.TermsAggregationCollectMode;
import org.opensearch.protobufs.TermsAggregationExecutionHint;
import org.opensearch.protobufs.TermsAggregationFields;
import org.opensearch.protobufs.TermsInclude;
import org.opensearch.protobufs.TermsPartition;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link TermsAggregationBuilderConverter}.
 */
public class TermsAggregationBuilderConverterTests extends OpenSearchTestCase {

    private TermsAggregationBuilderConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermsAggregationBuilderConverter();
    }

    public void testGetHandledAggregationCase() {
        assertEquals(
            AggregationContainer.AggregationContainerCase.TERMS_AGGREGATION,
            converter.getHandledAggregationCase()
        );
    }

    public void testBasicFieldOnly() {
        AggregationContainer container = buildContainer(
            TermsAggregationFields.newBuilder().setField("status").build()
        );

        AggregationBuilder builder = converter.fromProto("by_status", container);
        assertNotNull(builder);
        assertTrue(builder instanceof TermsAggregationBuilder);
        assertEquals("by_status", builder.getName());
        assertEquals("status", ((TermsAggregationBuilder) builder).field());
    }

    public void testScriptOnly() {
        AggregationContainer container = buildContainer(
            TermsAggregationFields.newBuilder()
                .setScript(
                    Script.newBuilder()
                        .setInline(InlineScript.newBuilder().setSource("doc['status'].value").build())
                        .build()
                )
                .build()
        );

        TermsAggregationBuilder termsBuilder = (TermsAggregationBuilder) converter.fromProto("t", container);
        assertNotNull(termsBuilder.script());
        assertEquals("doc['status'].value", termsBuilder.script().getIdOrCode());
    }

    public void testAllFieldsCombined() {
        AggregationContainer container = buildContainer(
            TermsAggregationFields.newBuilder()
                .setField("category")
                .setSize(50)
                .setShardSize(200)
                .setMinDocCount(10)
                .setShardMinDocCount(3)
                .setShowTermDocCountError(true)
                .setFormat("###")
                .setExecutionHint(TermsAggregationExecutionHint.TERMS_AGGREGATION_EXECUTION_HINT_MAP)
                .setCollectMode(TermsAggregationCollectMode.TERMS_AGGREGATION_COLLECT_MODE_BREADTH_FIRST)
                .addOrder(SortOrderSingleMap.newBuilder().setField("_count").setSortOrder(SortOrder.SORT_ORDER_ASC).build())
                .addExclude("other")
                .build()
        );

        TermsAggregationBuilder t = (TermsAggregationBuilder) converter.fromProto("full_terms", container);
        assertEquals("category", t.field());
        assertEquals(50, t.size());
        assertEquals(200, t.shardSize());
        assertEquals(10, t.minDocCount());
        assertEquals(3, t.shardMinDocCount());
        assertTrue(t.showTermDocCountError());
        assertEquals("###", t.format());
        assertEquals("map", t.executionHint());
        assertEquals(SubAggCollectionMode.BREADTH_FIRST, t.collectMode());
        assertNotNull(t.includeExclude());
    }

    public void testOrderVariants() {
        // _count desc
        TermsAggregationBuilder countDesc = (TermsAggregationBuilder) converter.fromProto("t", buildContainer(
            TermsAggregationFields.newBuilder().setField("f")
                .addOrder(SortOrderSingleMap.newBuilder().setField("_count").setSortOrder(SortOrder.SORT_ORDER_DESC).build())
                .build()
        ));
        assertEquals(BucketOrder.compound(BucketOrder.count(false)), countDesc.order());

        // _key asc (key order is a tie-breaker itself, not wrapped in compound)
        TermsAggregationBuilder keyAsc = (TermsAggregationBuilder) converter.fromProto("t", buildContainer(
            TermsAggregationFields.newBuilder().setField("f")
                .addOrder(SortOrderSingleMap.newBuilder().setField("_key").setSortOrder(SortOrder.SORT_ORDER_ASC).build())
                .build()
        ));
        assertEquals(BucketOrder.key(true), keyAsc.order());

        // sub-aggregation path
        TermsAggregationBuilder subAgg = (TermsAggregationBuilder) converter.fromProto("t", buildContainer(
            TermsAggregationFields.newBuilder().setField("f")
                .addOrder(SortOrderSingleMap.newBuilder().setField("avg_price").setSortOrder(SortOrder.SORT_ORDER_DESC).build())
                .build()
        ));
        assertEquals(BucketOrder.compound(BucketOrder.aggregation("avg_price", false)), subAgg.order());
    }

    public void testIncludeExcludeVariants() {
        // include by terms
        TermsAggregationBuilder withInclude = (TermsAggregationBuilder) converter.fromProto("t", buildContainer(
            TermsAggregationFields.newBuilder().setField("f")
                .setInclude(TermsInclude.newBuilder()
                    .setTerms(StringArray.newBuilder().addStringArray("a").addStringArray("b").build())
                    .build())
                .build()
        ));
        assertNotNull(withInclude.includeExclude());

        // include by partition
        TermsAggregationBuilder withPartition = (TermsAggregationBuilder) converter.fromProto("t", buildContainer(
            TermsAggregationFields.newBuilder().setField("f")
                .setInclude(TermsInclude.newBuilder()
                    .setPartition(TermsPartition.newBuilder().setPartition(0).setNumPartitions(5).build())
                    .build())
                .build()
        ));
        assertNotNull(withPartition.includeExclude());
        assertTrue(withPartition.includeExclude().isPartitionBased());

        // exclude only
        TermsAggregationBuilder withExclude = (TermsAggregationBuilder) converter.fromProto("t", buildContainer(
            TermsAggregationFields.newBuilder().setField("f")
                .addExclude("x").addExclude("y")
                .build()
        ));
        assertNotNull(withExclude.includeExclude());
    }

    public void testContainerWithoutTermsAggregation() {
        AggregationContainer container = AggregationContainer.newBuilder().build();
        expectThrows(IllegalStateException.class, () -> converter.fromProto("t", container));
    }

    public void testMissingTermsFields() {
        AggregationContainer container = AggregationContainer.newBuilder()
            .setTermsAggregation(TermsAggregation.newBuilder().build())
            .build();
        expectThrows(IllegalArgumentException.class, () -> converter.fromProto("t", container));
    }

    private static AggregationContainer buildContainer(TermsAggregationFields fields) {
        return AggregationContainer.newBuilder()
            .setTermsAggregation(TermsAggregation.newBuilder().setTerms(fields).build())
            .build();
    }
}
