/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Coordinator filtered-alias guard behaviour, exercised with a real {@link IndexNameExpressionResolver}
 * so the hidden-alias expansion path is genuinely covered.
 */
public class FilteringAliasGuardTests extends OpenSearchTestCase {

    private static final String FILTER = "{ \"term\": { \"tenant\": \"a\" } }";

    private final IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

    /** A visible index reached through a plain-named filtering alias must be rejected with 400. */
    public void testFilteringAliasByPlainNameRejected() {
        ClusterState state = state();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FilteringAliasGuard.check(
                resolver,
                state,
                new String[] { "filtered-alias" },
                IndicesOptions.lenientExpandOpen(),
                List.of(state.metadata().index("logs-1"))
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("filtered-alias"));
        assertTrue(e.getMessage(), e.getMessage().contains("logs-1"));
    }

    /** A filtering alias reached through a wildcard must be rejected with 400. */
    public void testFilteringAliasByWildcardRejected() {
        ClusterState state = state();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FilteringAliasGuard.check(
                resolver,
                state,
                new String[] { "filtered-*" },
                IndicesOptions.lenientExpandOpen(),
                List.of(state.metadata().index("logs-1"))
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("filtered-alias"));
    }

    /**
     * A HIDDEN filtering alias reached via {@code expand_wildcards=open,hidden} must be rejected — the
     * case the lenientExpandOpen resolution silently dropped, letting the filter leak.
     */
    public void testHiddenFilteringAliasByWildcardRejected() {
        ClusterState state = state();
        IndicesOptions openAndHidden = IndicesOptions.fromOptions(true, true, true, false, true);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FilteringAliasGuard.check(
                resolver,
                state,
                new String[] { "shadow-*" },
                openAndHidden,
                List.of(state.metadata().index("archive-1"))
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("shadow-view"));
        assertTrue(e.getMessage(), e.getMessage().contains("archive-1"));
    }

    /** A non-filtering alias must NOT be rejected. */
    public void testNonFilteringAliasAllowed() {
        ClusterState state = state();
        FilteringAliasGuard.check(
            resolver,
            state,
            new String[] { "plain-alias" },
            IndicesOptions.lenientExpandOpen(),
            List.of(state.metadata().index("logs-1"))
        );
    }

    private static ClusterState state() {
        Metadata.Builder metadata = Metadata.builder()
            .put(
                indexBuilder("logs-1", Settings.EMPTY).putAlias(AliasMetadata.builder("filtered-alias").filter(FILTER))
                    .putAlias(AliasMetadata.builder("plain-alias"))
            )
            .put(
                indexBuilder("archive-1", Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build()).putAlias(
                    AliasMetadata.builder("shadow-view").isHidden(true).filter(FILTER)
                )
            );
        return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
    }

    private static IndexMetadata.Builder indexBuilder(String index, Settings additionalSettings) {
        return IndexMetadata.builder(index)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(additionalSettings)
            );
    }
}
