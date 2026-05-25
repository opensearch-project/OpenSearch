/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link IndexResolution} — the helper that expands a table name
 * (concrete index or alias) to a list of {@link IndexMetadata} and validates
 * mapping compatibility across all backing indices.
 */
public class IndexResolutionTests extends OpenSearchTestCase {

    private static final IndexNameExpressionResolver RESOLVER = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

    public void testConcreteIndexResolvesToSingleton() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));
        IndexResolution result = IndexResolution.resolve("bank", state);
        assertEquals(1, result.concreteIndices().size());
        assertEquals("bank", result.concreteIndices().get(0).getIndex().getName());
        assertEquals("bank", result.requestedName());
    }

    public void testAliasResolvesToAllBackingIndicesWhenSchemasMatch() {
        IndexMetadata.Builder a = indexBuilder("bank_a", longField("age")).putAlias(AliasMetadata.builder("bank_all").build());
        IndexMetadata.Builder b = indexBuilder("bank_b", longField("age")).putAlias(AliasMetadata.builder("bank_all").build());
        ClusterState state = clusterStateOf(a, b);

        IndexResolution result = IndexResolution.resolve("bank_all", state);

        assertEquals(2, result.concreteIndices().size());
        assertEquals("bank_all", result.requestedName());
        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder("bank_a", "bank_b"));
    }

    public void testAliasRejectsWhenFieldTypesDiffer() {
        IndexMetadata.Builder a = indexBuilder("bank_a", longField("age")).putAlias(AliasMetadata.builder("bank_all").build());
        IndexMetadata.Builder b = indexBuilder("bank_b", keywordField("age")).putAlias(AliasMetadata.builder("bank_all").build());
        ClusterState state = clusterStateOf(a, b);

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> IndexResolution.resolve("bank_all", state));
        assertTrue("error must mention the conflicting field: " + ex.getMessage(), ex.getMessage().contains("age"));
        assertTrue(
            "error must mention both indices: " + ex.getMessage(),
            ex.getMessage().contains("bank_a") && ex.getMessage().contains("bank_b")
        );
    }

    public void testAliasAcceptsFieldsThatMapToSameCalciteType() {
        // text and keyword both map to VARCHAR, so a union over them is schema-compatible even
        // though the raw OpenSearch type strings differ. The schema builder collapses them too.
        IndexMetadata.Builder a = indexBuilder("bank_a", textField("name")).putAlias(AliasMetadata.builder("bank_all").build());
        IndexMetadata.Builder b = indexBuilder("bank_b", keywordField("name")).putAlias(AliasMetadata.builder("bank_all").build());
        ClusterState state = clusterStateOf(a, b);

        IndexResolution result = IndexResolution.resolve("bank_all", state);

        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder("bank_a", "bank_b"));
    }

    public void testAliasSkipsClosedBackingIndices() {
        IndexMetadata.Builder open = indexBuilder("bank_a", longField("age")).putAlias(AliasMetadata.builder("bank_all").build());
        IndexMetadata.Builder closed = indexBuilder("bank_b", longField("age")).state(IndexMetadata.State.CLOSE)
            .putAlias(AliasMetadata.builder("bank_all").build());
        ClusterState state = clusterStateOf(open, closed);

        IndexResolution result = IndexResolution.resolve("bank_all", state);

        assertEquals("closed backing index must be excluded", 1, result.concreteIndices().size());
        assertEquals("bank_a", result.concreteIndices().get(0).getIndex().getName());
    }

    public void testAliasResolvingOnlyToClosedIndicesThrows() {
        IndexMetadata.Builder closed = indexBuilder("bank_a", longField("age")).state(IndexMetadata.State.CLOSE)
            .putAlias(AliasMetadata.builder("bank_all").build());
        ClusterState state = clusterStateOf(closed);

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> IndexResolution.resolve("bank_all", state));
        assertTrue("error must mention closed: " + ex.getMessage(), ex.getMessage().toLowerCase().contains("closed"));
    }

    public void testAliasRejectsFilterAlias() {
        AliasMetadata filterAlias = AliasMetadata.builder("active_only").filter("{\"term\":{\"status\":\"active\"}}").build();
        IndexMetadata.Builder a = indexBuilder("bank_a", longField("age")).putAlias(filterAlias);
        ClusterState state = clusterStateOf(a);

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> IndexResolution.resolve("active_only", state));
        assertTrue("error must mention the alias name: " + ex.getMessage(), ex.getMessage().contains("active_only"));
        assertTrue("error must mention 'filter': " + ex.getMessage(), ex.getMessage().toLowerCase().contains("filter"));
    }

    public void testMissingNameThrows() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> IndexResolution.resolve("does_not_exist", state));
        assertTrue("error must mention the missing name: " + ex.getMessage(), ex.getMessage().contains("does_not_exist"));
    }

    public void testWildcardResolvesToMatchingConcreteIndices() {
        ClusterState state = clusterStateOf(
            indexBuilder("test", longField("age")),
            indexBuilder("test1", longField("age")),
            indexBuilder("other", longField("age"))
        );

        IndexResolution result = IndexResolution.resolve("test*", state, RESOLVER);

        assertEquals("test*", result.requestedName());
        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder("test", "test1"));
    }

    public void testCommaSeparatedExpressionResolvesToUnion() {
        ClusterState state = clusterStateOf(
            indexBuilder("bank", longField("balance")),
            indexBuilder("test", longField("age")),
            indexBuilder("other", longField("age"))
        );

        IndexResolution result = IndexResolution.resolve("bank,test", state, RESOLVER);

        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder("bank", "test"));
    }

    public void testWildcardRejectsIncompatibleSchemasAcrossMatches() {
        ClusterState state = clusterStateOf(indexBuilder("test", longField("age")), indexBuilder("test1", keywordField("age")));

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> IndexResolution.resolve("test*", state, RESOLVER));
        assertTrue("error must mention the conflicting field: " + ex.getMessage(), ex.getMessage().contains("age"));
    }

    public void testWildcardMatchingNothingThrows() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IndexResolution.resolve("nomatch*", state, RESOLVER)
        );
        assertTrue("error must mention the unmatched expression: " + ex.getMessage(), ex.getMessage().contains("nomatch*"));
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

    private static String keywordField(String name) {
        return "{\"properties\":{\"" + name + "\":{\"type\":\"keyword\"}}}";
    }

    private static String textField(String name) {
        return "{\"properties\":{\"" + name + "\":{\"type\":\"text\"}}}";
    }

    private static ClusterState clusterStateOf(IndexMetadata.Builder... indices) {
        Metadata.Builder mb = Metadata.builder();
        for (IndexMetadata.Builder b : indices) {
            mb.put(b);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(mb.build()).build();
    }
}
