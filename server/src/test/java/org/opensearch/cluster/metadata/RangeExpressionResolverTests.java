/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;

import static org.opensearch.common.util.set.Sets.newHashSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class RangeExpressionResolverTests extends OpenSearchTestCase {
    public void testRangeExpression() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-2022.01.01"))
            .put(indexBuilder("test-2022.01.02"))
            .put(indexBuilder("test-2022.01.03"))
            .put(indexBuilder("test-2022.01.04"))
            .put(indexBuilder("test-"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.RangeExpressionResolver resolver = new IndexNameExpressionResolver.RangeExpressionResolver();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled(),
            false
        );

        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-2022.01.01"))),
            equalTo(newHashSet("test-2022.01.01"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Arrays.asList("test-2022.01.01", "test-2022.01.02"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2022.01.01|2022.01.03]"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-(2022.01.01|2022.01.03]"))),
            equalTo(newHashSet("test-2022.01.02", "test-2022.01.03"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2022.01.01|2022.01.03)"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-(2022.01.01|2022.01.03)"))),
            equalTo(newHashSet("test-2022.01.02"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("[test-2022.01.01|test-2022.01.03)"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02"))
        );

        // unbounded on one side
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-(|2022.01.03]"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[|2022.01.03]"))),
            equalTo(newHashSet("test-", "test-2022.01.01", "test-2022.01.02", "test-2022.01.03"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2022.01.01|]"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03", "test-2022.01.04"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2022.01.01|)"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03", "test-2022.01.04"))
        );

        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2022.01.01|2022.01.01)"))),
            equalTo(Collections.emptySet())
        );
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2022.01.01|2022.01.02)"))),
            equalTo(newHashSet("test-2022.01.01"))
        );
        assertThat(
            newHashSet(resolver.resolve(context, Arrays.asList("test-[2022.01.01|2022.01.02)", "test-(2022.01.02|2022.01.03]"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.03"))
        );

        assertThat(
            newHashSet(resolver.resolve(context, Arrays.asList("test-[2022.01.01|2022.01.03]", "test-[2022.01.02|2022.01.04]"))),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03", "test-2022.01.04"))
        );

        // right bound can not greater to left bound
        assertEquals(
            "fromKey > toKey",
            expectThrows(
                IllegalArgumentException.class,
                () -> resolver.resolve(context, Collections.singletonList("test-[2022.01.05|2022.01.03]"))
            ).getMessage()
        );

        assertThat(
            expectThrows(
                OpenSearchParseException.class,
                () -> resolver.resolve(context, Collections.singletonList("test-2022.01.01|2022.01.03]"))
            ).getMessage(),
            startsWith("Invalid range expression,")
        );
        assertThat(
            expectThrows(
                OpenSearchParseException.class,
                () -> resolver.resolve(context, Collections.singletonList("test-[2022.01.01|2022.01.03"))
            ).getMessage(),
            startsWith("Invalid range expression,")
        );

        // out of scope
        assertThat(
            newHashSet(resolver.resolve(context, Collections.singletonList("test-[2099.01.01|2099.01.03)"))),
            equalTo(Collections.emptySet())
        );
    }

    public void testRangeAndWildCardExpression() {
        Metadata.Builder mdBuilder = Metadata.builder()
            .put(indexBuilder("test-2022.01.01"))
            .put(indexBuilder("test-2022.01.02"))
            .put(indexBuilder("test-2022.01.03"))
            .put(indexBuilder("test-2022.01.04"))
            .put(indexBuilder("test-2022.01.05").state(IndexMetadata.State.CLOSE));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metadata(mdBuilder).build();
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.strictExpandOpenAndForbidClosedIgnoreThrottled(),
            false
        );

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));

        String[] concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(context, "test-[2099.01.01|2099.01.03)");
        assertThat(newHashSet(concreteIndexNames), equalTo(Collections.emptySet()));

        concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(context, "test-[2099.01.01|2099.01.03)", "test-*");
        assertThat(
            newHashSet(concreteIndexNames),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03", "test-2022.01.04"))
        );

        concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(context, "test-[2022.01.01|2022.01.05]", "test-2022*");
        assertThat(
            newHashSet(concreteIndexNames),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03", "test-2022.01.04"))
        );

        // open and closed
        IndexNameExpressionResolver.Context contextOpenAndClose = new IndexNameExpressionResolver.Context(
            state,
            IndicesOptions.lenientExpand(),
            false
        );
        concreteIndexNames = indexNameExpressionResolver.concreteIndexNames(contextOpenAndClose, "test-[2022.01.01|2022.01.05]");
        assertThat(
            newHashSet(concreteIndexNames),
            equalTo(newHashSet("test-2022.01.01", "test-2022.01.02", "test-2022.01.03", "test-2022.01.04", "test-2022.01.05"))
        );
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
    }
}
