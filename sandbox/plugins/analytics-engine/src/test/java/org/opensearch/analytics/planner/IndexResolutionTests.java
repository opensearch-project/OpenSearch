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
import org.opensearch.cluster.DataStreamTestHelper;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Locale;

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
        assertTrue("error must mention closed: " + ex.getMessage(), ex.getMessage().toLowerCase(Locale.ROOT).contains("closed"));
    }

    public void testAliasRejectsFilterAlias() {
        AliasMetadata filterAlias = AliasMetadata.builder("active_only").filter("{\"term\":{\"status\":\"active\"}}").build();
        IndexMetadata.Builder a = indexBuilder("bank_a", longField("age")).putAlias(filterAlias);
        ClusterState state = clusterStateOf(a);

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> IndexResolution.resolve("active_only", state));
        assertTrue("error must mention the alias name: " + ex.getMessage(), ex.getMessage().contains("active_only"));
        assertTrue("error must mention 'filter': " + ex.getMessage(), ex.getMessage().toLowerCase(Locale.ROOT).contains("filter"));
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

    // TODO honor IndicesOptions.allowNoIndices() — vanilla OpenSearch lets `ignore_unavailable=true`
    // / `allow_no_indices=true` callers see an empty result for wildcards matching nothing. This
    // path always throws, which diverges. Decide the analytics-engine contract (always-strict vs.
    // pass-through), then either flip this test or add a permissive overload.
    public void testWildcardMatchingNothingThrows() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IndexResolution.resolve("nomatch*", state, RESOLVER)
        );
        assertTrue("error must mention the unmatched expression: " + ex.getMessage(), ex.getMessage().contains("nomatch*"));
    }

    public void testMissingConcreteNameWithResolverStillThrows() {
        ClusterState state = clusterStateOf(indexBuilder("bank", longField("age")));
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IndexResolution.resolve("does_not_exist", state, RESOLVER)
        );
        assertTrue("error must mention the missing name: " + ex.getMessage(), ex.getMessage().contains("does_not_exist"));
    }

    /**
     * Closed indices named directly are rejected with the same error style as alias-only-backed
     * closed indices, so all three resolution paths (literal-name, alias, wildcard) converge on
     * "closed indices are not searchable through analytics-engine."
     */
    public void testConcreteClosedIndexIsRejected() {
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(closedIndexBuilder("frozen", longField("age"))).build())
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> IndexResolution.resolve("frozen", state, RESOLVER)
        );
        assertTrue("error must name the closed index: " + ex.getMessage(), ex.getMessage().contains("frozen"));
        assertTrue("error must say closed: " + ex.getMessage(), ex.getMessage().toLowerCase(Locale.ROOT).contains("closed"));
    }

    /**
     * Pins the documented gap in {@link IndexResolution} schema-compatibility validation: the
     * top-level "properties" walk does NOT recurse into nested object types, so two indices that
     * declare {@code a} as a different leaf type under an object disagree at the data-node bind
     * step instead of at resolution time.
     *
     * <p>TODO: extend {@code validateSchemaCompatibility} to descend into object {@code properties}
     * trees, OR document this as deferred-to-bind-time and add a regression test for the bind-time
     * error path. Today the conflict is silently accepted at resolution.
     */
    public void testNestedFieldTypeMismatchIsNotCaughtAtResolution() {
        // a.b is long in index "left", keyword in index "right" — but a itself is "object" in both,
        // and validateSchemaCompatibility only compares the object/object pair at top level.
        IndexMetadata.Builder left = indexBuilder(
            "left",
            "{\"properties\":{\"a\":{\"type\":\"object\",\"properties\":{\"b\":{\"type\":\"long\"}}}}}"
        ).putAlias(AliasMetadata.builder("both").build());
        IndexMetadata.Builder right = indexBuilder(
            "right",
            "{\"properties\":{\"a\":{\"type\":\"object\",\"properties\":{\"b\":{\"type\":\"keyword\"}}}}}"
        ).putAlias(AliasMetadata.builder("both").build());
        ClusterState state = clusterStateOf(left, right);

        IndexResolution result = IndexResolution.resolve("both", state);

        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder("left", "right"));
    }

    /**
     * Data streams fan out to their backing indices, just like aliases. The same closed-filter
     * and schema-compat checks apply — DS backings should have identical mappings by construction,
     * but a manual amendment could drift, so we still validate.
     */
    public void testDataStreamResolvesToBackingIndices() {
        IndexMetadata b1 = indexBuilder(DataStream.getDefaultBackingIndexName("logs", 1), longField("age")).build();
        IndexMetadata b2 = indexBuilder(DataStream.getDefaultBackingIndexName("logs", 2), longField("age")).build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(b1, false)
                    .put(b2, false)
                    .put(
                        new DataStream(
                            "logs",
                            DataStreamTestHelper.createTimestampField("@timestamp"),
                            List.of(b1.getIndex(), b2.getIndex())
                        )
                    )
                    .build()
            )
            .build();

        IndexResolution result = IndexResolution.resolve("logs", state, RESOLVER);

        assertEquals("logs", result.requestedName());
        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder(b1.getIndex().getName(), b2.getIndex().getName()));
    }

    /**
     * Data stream backings with conflicting mappings fail the same schema-compat check as aliases.
     * (DS backings *should* be identical by construction, but manual mapping amendments can drift.)
     */
    public void testDataStreamRejectsConflictingBackingMappings() {
        IndexMetadata b1 = indexBuilder(DataStream.getDefaultBackingIndexName("logs", 1), longField("age")).build();
        IndexMetadata b2 = indexBuilder(DataStream.getDefaultBackingIndexName("logs", 2), keywordField("age")).build();
        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .put(b1, false)
                    .put(b2, false)
                    .put(
                        new DataStream(
                            "logs",
                            DataStreamTestHelper.createTimestampField("@timestamp"),
                            List.of(b1.getIndex(), b2.getIndex())
                        )
                    )
                    .build()
            )
            .build();

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> IndexResolution.resolve("logs", state, RESOLVER));
        assertTrue("error must mention the conflicting field: " + ex.getMessage(), ex.getMessage().contains("age"));
    }

    public void testExclusionPatternResolvesCorrectly() {
        ClusterState state = clusterStateOf(
            indexBuilder("test", longField("age")),
            indexBuilder("test1", longField("age")),
            indexBuilder("test2", longField("age"))
        );

        IndexResolution result = IndexResolution.resolve("test*,-test2", state, RESOLVER);

        assertThat(result.concreteIndexNames(), org.hamcrest.Matchers.containsInAnyOrder("test", "test1"));
        assertFalse("test2 must be excluded", result.concreteIndexNames().contains("test2"));
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static IndexMetadata.Builder closedIndexBuilder(String name, String mappingJson) {
        return indexBuilder(name, mappingJson).state(IndexMetadata.State.CLOSE);
    }

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
