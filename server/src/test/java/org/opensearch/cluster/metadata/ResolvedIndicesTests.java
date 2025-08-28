/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class ResolvedIndicesTests extends OpenSearchTestCase {

    public void testOfNames() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of("a", "b", "c");
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("a", "b", "c"));
        assertThat(Arrays.asList(resolvedIndices.local().namesAsArray()), containsInAnyOrder("a", "b", "c"));
        assertFalse(resolvedIndices.local().isEmpty());
        assertTrue(resolvedIndices.remote().isEmpty());
        assertFalse(resolvedIndices.isEmpty());
    }

    public void testOfConcreteIndices() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of(new Index("index_a", "uuid_a"), new Index("index_b", "uuid_b"));
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("index_a", "index_b"));
        assertThat(resolvedIndices.local(), isA(ResolvedIndices.Local.Concrete.class));
        ResolvedIndices.Local.Concrete concrete = (ResolvedIndices.Local.Concrete) resolvedIndices.local();
        assertThat(concrete.concreteIndices(), containsInAnyOrder(new Index("index_a", "uuid_a"), new Index("index_b", "uuid_b")));
    }

    public void testOfNonNull() {
        ResolvedIndices resolvedIndices = ResolvedIndices.ofNonNull("a", "b", "c", null);
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("a", "b", "c"));
    }

    public void testWithLocalOriginalIndices() {
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "x", "y" }, IndicesOptions.LENIENT_EXPAND_OPEN);
        ResolvedIndices resolvedIndices = ResolvedIndices.of("a", "b", "c").withLocalOriginalIndices(originalIndices);
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("a", "b", "c"));
        assertArrayEquals(resolvedIndices.local().originalIndices().indices(), new String[] { "x", "y" });
    }

    public void testOfEmpty() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of(new String[0]);
        assertTrue(resolvedIndices.local().isEmpty());
        assertTrue(resolvedIndices.remote().isEmpty());
        assertTrue(resolvedIndices.isEmpty());
    }

    public void testWithRemoteIndices() {
        Map<String, OriginalIndices> remoteIndices = Map.of(
            "remote_cluster",
            new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN)
        );
        ResolvedIndices resolvedIndices = ResolvedIndices.of(new String[0]).withRemoteIndices(remoteIndices);
        assertTrue(resolvedIndices.remote().asClusterToOriginalIndicesMap().containsKey("remote_cluster"));
        assertArrayEquals(
            new String[] { "remote_index" },
            resolvedIndices.remote().asClusterToOriginalIndicesMap().get("remote_cluster").indices()
        );
        assertThat(resolvedIndices.remote().asRawExpressions(), containsInAnyOrder("remote_cluster:remote_index"));
    }

    public void testLocalConcreteOf() {
        Set<Index> indices = Set.of(new Index("index_a", "uuid_a"), new Index("index_b", "uuid_b"));
        Set<String> names = Set.of("index_a", "index_b", "index_c");
        ResolvedIndices.Local.Concrete concrete = ResolvedIndices.Local.Concrete.of(indices, names).withResolutionErrors();
        assertThat(concrete.concreteIndices(), containsInAnyOrder(new Index("index_a", "uuid_a"), new Index("index_b", "uuid_b")));
        assertThat(concrete.namesOfConcreteIndices(), containsInAnyOrder("index_a", "index_b"));
        assertThat(Arrays.asList(concrete.namesOfConcreteIndicesAsArray()), containsInAnyOrder("index_a", "index_b"));
        assertThat(concrete.names(), containsInAnyOrder("index_a", "index_b", "index_c"));
    }

    public void testLocalConcreteWithResolutionErrors() {
        Set<Index> indices = Set.of(new Index("index_a", "uuid_a"), new Index("index_b", "uuid_b"));
        Set<String> names = Set.of("index_a", "index_b", "index_c");
        ResolvedIndices.Local.Concrete concrete = ResolvedIndices.Local.Concrete.of(indices, names)
            .withResolutionErrors(new IndexNotFoundException("index_x"));
        assertThat(concrete.names(), containsInAnyOrder("index_a", "index_b", "index_c"));
        IndexNotFoundException exception = expectThrows(IndexNotFoundException.class, concrete::concreteIndices);
        assertThat(exception.getIndex().getName(), equalTo("index_x"));
        concrete = concrete.withoutResolutionErrors();
        assertThat(concrete.concreteIndices(), containsInAnyOrder(new Index("index_a", "uuid_a"), new Index("index_b", "uuid_b")));
    }

}
