/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

/**
 * Integration tests for force merge behavior in the DataFormatAwareEngine
 * via the composite engine.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeForceMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-force-merge";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings compositeIndexSettings() {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    public void testForceMergeReducesSegmentsToOne() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeIndexSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 10;
        int cycles = 5;
        for (int cycle = 0; cycle < cycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                client().prepareIndex(INDEX_NAME).setSource("name", randomAlphaOfLength(8), "age", randomIntBetween(1, 100)).get();
            }
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        long segmentsBefore = getSegmentCount();
        assertTrue("Expected multiple segments before merge, got " + segmentsBefore, segmentsBefore > 1);

        ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertEquals(0, response.getFailedShards());

        long segmentsAfter = getSegmentCount();
        assertEquals("Expected exactly 1 segment after force merge to 1", 1, segmentsAfter);
        assertEquals(docsPerCycle * cycles, getTotalDocCount());
    }

    public void testForceMergeWithMaxNumSegmentsGreaterThanOne() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeIndexSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int totalDocs = 20;
        // Index one doc per refresh to create many small segments
        for (int i = 0; i < totalDocs; i++) {
            client().prepareIndex(INDEX_NAME).setSource("name", randomAlphaOfLength(8), "age", randomIntBetween(1, 100)).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        long segmentsBefore = getSegmentCount();
        assertTrue("Expected many segments, got " + segmentsBefore, segmentsBefore > 5);

        int maxNumSegments = 5;
        ForceMergeResponse response = client().admin()
            .indices()
            .prepareForceMerge(INDEX_NAME)
            .setMaxNumSegments(maxNumSegments)
            .setFlush(true)
            .get();
        assertEquals(0, response.getFailedShards());

        long segmentsAfter = getSegmentCount();
        assertTrue("Expected segments <= " + maxNumSegments + " after force merge, got " + segmentsAfter, segmentsAfter <= maxNumSegments);
        assertTrue("Expected segments reduced, before=" + segmentsBefore + " after=" + segmentsAfter, segmentsAfter < segmentsBefore);
        assertEquals(totalDocs, getTotalDocCount());
    }

    public void testForceMergeFlushesBeforeMerging() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeIndexSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int totalDocs = 20;
        // Index docs without refresh — data sits in indexing buffer
        for (int i = 0; i < totalDocs; i++) {
            client().prepareIndex(INDEX_NAME).setSource("name", randomAlphaOfLength(8), "age", randomIntBetween(1, 100)).get();
        }

        // Force merge with flush=true should flush unflushed data first
        ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertEquals(0, response.getFailedShards());

        // Verify all docs are present via catalog snapshot
        assertEquals(totalDocs, getTotalDocCount());
    }

    public void testForceMergeOnEmptyIndex() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(compositeIndexSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertEquals(0, response.getFailedShards());
        assertEquals(0, getTotalDocCount());
    }

    private long getSegmentCount() {
        DataFormatAwareEngine engine = CompositeEngineHelper.getEngine(internalCluster().clusterService(), internalCluster(), INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> snapshotRef = engine.acquireLastCommittedSnapshot(false)) {
            return snapshotRef.get().getSegments().size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private long getTotalDocCount() {
        DataFormatAwareEngine engine = CompositeEngineHelper.getEngine(internalCluster().clusterService(), internalCluster(), INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> snapshotRef = engine.acquireLastCommittedSnapshot(false)) {
            return snapshotRef.get().getNumDocs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
