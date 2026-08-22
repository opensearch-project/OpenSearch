/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for the 4-arg {@link IndexResolution#resolve(String, ClusterState, IndexNameExpressionResolver, IndicesOptions)}
 * overload and regression guard for the 3-arg overload delegating with lenientExpandOpen.
 */
public class IndexResolutionIndicesOptionsTests extends OpenSearchTestCase {

    private static final IndexNameExpressionResolver RESOLVER = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

    /**
     * (a) 3-arg resolve still delegates to 4-arg with lenientExpandOpen — a nonexistent index
     * in a comma-list is silently dropped (lenient semantics).
     */
    public void testThreeArgResolveDelegatesToFourArgWithLenientExpandOpen() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));

        // "bank,missing" with lenient options → should resolve to just "bank" (lenient drops missing)
        IndexResolution result = IndexResolution.resolve("bank,missing", state, RESOLVER);

        assertEquals(1, result.concreteIndices().size());
        assertEquals("bank", result.concreteIndices().get(0).getIndex().getName());
    }

    /**
     * (b) 4-arg overload with STRICT_EXPAND_OPEN throws on nonexistent index in a comma-list,
     * unlike the lenient default.
     */
    public void testFourArgResolveWithStrictOptionsThrowsOnMissing() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));

        // strictExpandOpen forbids missing indices
        IndicesOptions strict = IndicesOptions.strictExpandOpen();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IndexResolution.resolve("bank,missing", state, RESOLVER, strict)
        );
        assertTrue("error must mention the expression: " + ex.getMessage(), ex.getMessage().contains("bank,missing"));
    }

    /**
     * (b-2) 4-arg overload with lenient options behaves same as 3-arg — drops missing silently.
     */
    public void testFourArgResolveWithLenientOptionsDropsMissing() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));

        IndexResolution result = IndexResolution.resolve("bank,missing", state, RESOLVER, IndicesOptions.lenientExpandOpen());

        assertEquals(1, result.concreteIndices().size());
        assertEquals("bank", result.concreteIndices().get(0).getIndex().getName());
    }

    /**
     * (c) PlannerContext carries IndicesOptions and the accessor returns what was set.
     */
    public void testPlannerContextCarriesIndicesOptions() {
        IndicesOptions custom = IndicesOptions.strictExpandOpen();
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
        CapabilityRegistry registry = null; // not needed for this test
        PlannerContext ctx = new PlannerContext(registry, state, null, false, true, Settings.EMPTY, s -> 0L, custom);

        assertSame(custom, ctx.getIndicesOptions());
    }

    /**
     * (c-2) PlannerContext without IndicesOptions defaults to lenientExpandOpen.
     */
    public void testPlannerContextDefaultsToLenientExpandOpen() {
        ClusterState state = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();
        PlannerContext ctx = new PlannerContext(null, state);

        assertEquals(IndicesOptions.lenientExpandOpen(), ctx.getIndicesOptions());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static IndexMetadata.Builder indexBuilder(String name, String mappingJson) {
        try {
            return IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put("index.version.created", org.opensearch.Version.CURRENT.id)
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .build()
                )
                .putMapping(mappingJson);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }

    private static String longField(String name) {
        return "{\"properties\":{\"" + name + "\":{\"type\":\"long\"}}}";
    }

    private static ClusterState clusterStateOf(IndexMetadata.Builder... indices) {
        Metadata.Builder mb = Metadata.builder();
        for (IndexMetadata.Builder b : indices) {
            mb.put(b);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(mb.build()).build();
    }
}
