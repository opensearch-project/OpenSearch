/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.admin.indices.delete.DeleteIndexAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;

public class ResolvedIndicesTests extends OpenSearchTestCase {

    public void testOfNames() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of("a", "b", "c");
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("a", "b", "c"));
        assertThat(resolvedIndices.local().names(ClusterState.EMPTY_STATE), containsInAnyOrder("a", "b", "c"));
        assertThat(Arrays.asList(resolvedIndices.local().namesAsArray()), containsInAnyOrder("a", "b", "c"));
        assertFalse(resolvedIndices.local().isEmpty());
        assertTrue(resolvedIndices.remote().isEmpty());
        assertFalse(resolvedIndices.isEmpty());
    }

    public void testOfNamesCollection() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of(List.of("a", "b"));
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("a", "b"));
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

    public void testUnknown() {
        OptionallyResolvedIndices resolvedIndices = ResolvedIndices.unknown();
        assertFalse(resolvedIndices instanceof ResolvedIndices);
        resolvedIndices = OptionallyResolvedIndices.unknown();
        assertFalse(resolvedIndices instanceof ResolvedIndices);
        assertFalse(resolvedIndices.local().isEmpty());
        assertTrue(resolvedIndices.local().contains("whatever"));
        assertTrue(resolvedIndices.local().containsAny(List.of("whatever")));
        assertTrue(resolvedIndices.local().containsAny(i -> false));
        assertEquals("ResolvedIndices{unknown=true}", resolvedIndices.toString());
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
        assertEquals(resolvedIndices.remote().asRawExpressions(), Arrays.asList(resolvedIndices.remote().asRawExpressionsArray()));
    }

    public void testWithoutRemoteIndices() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of(new String[0]);
        assertTrue(resolvedIndices.remote().asClusterToOriginalIndicesMap().isEmpty());
        assertTrue(resolvedIndices.remote().asRawExpressions().isEmpty());
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

    public void testWithLocalSubActions() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of("a", "b", "c")
            .withLocalSubActions(DeleteIndexAction.INSTANCE, ResolvedIndices.Local.of("x", "y", "z"));
        assertThat(resolvedIndices.local().names(), containsInAnyOrder("a", "b", "c"));
        assertThat(resolvedIndices.local().subActions().get(DeleteIndexAction.NAME).names(), containsInAnyOrder("x", "y", "z"));
    }

    public void testLocalOf() {
        ResolvedIndices.Local local = ResolvedIndices.Local.of(List.of("o", "p", "q"));
        assertThat(local.names(), containsInAnyOrder("o", "p", "q"));
    }

    public void testNamesWithClusterState() {
        Metadata.Builder metadata = Metadata.builder()
            .put(indexBuilder("index_a1").putAlias(AliasMetadata.builder("alias_a")))
            .put(indexBuilder("index_a2").putAlias(AliasMetadata.builder("alias_a")))
            .put(indexBuilder("index_b1"));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        ResolvedIndices resolvedIndices = ResolvedIndices.of("index_not_existing", "alias_a", "index_b1");
        assertThat(resolvedIndices.local().names(clusterState), containsInAnyOrder("index_not_existing", "alias_a", "index_b1"));
        assertThat(
            ResolvedIndices.unknown().local().names(clusterState),
            containsInAnyOrder("index_a1", "index_a2", "index_b1", "alias_a")
        );
    }

    public void testNamesOfIndices() {
        Metadata.Builder metadata = Metadata.builder()
            .put(indexBuilder("index_a1").putAlias(AliasMetadata.builder("alias_a")))
            .put(indexBuilder("index_a2").putAlias(AliasMetadata.builder("alias_a")))
            .put(indexBuilder("index_b1"));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).metadata(metadata).build();
        ResolvedIndices resolvedIndices = ResolvedIndices.of("index_not_existing", "alias_a", "index_b1");
        assertThat(
            resolvedIndices.local().namesOfIndices(clusterState),
            containsInAnyOrder("index_not_existing", "index_a1", "index_a2", "index_b1")
        );
    }

    public void testContains() {
        ResolvedIndices resolvedIndices = ResolvedIndices.of("a", "b", "c");
        assertTrue(resolvedIndices.local().contains("a"));
        assertFalse(resolvedIndices.local().contains("x"));
        assertTrue(resolvedIndices.local().containsAny(List.of("a", "x")));
        assertFalse(resolvedIndices.local().containsAny(List.of("x", "y")));
        assertTrue(resolvedIndices.local().containsAny(i -> i.equals("a")));
        assertFalse(resolvedIndices.local().containsAny(i -> i.equals("z")));
    }

    public void testEqualsHashCode() {
        ResolvedIndices resolvedIndices1 = ResolvedIndices.of("index")
            .withRemoteIndices(Map.of("remote", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN)))
            .withLocalSubActions(DeleteIndexAction.INSTANCE, ResolvedIndices.Local.of("other"));
        ResolvedIndices resolvedIndices2 = ResolvedIndices.of("index")
            .withRemoteIndices(Map.of("remote", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN)))
            .withLocalSubActions(DeleteIndexAction.INSTANCE, ResolvedIndices.Local.of("other"));
        assertEquals(resolvedIndices1.hashCode(), resolvedIndices2.hashCode());
        assertTrue(resolvedIndices1 + " equals " + resolvedIndices2, resolvedIndices1.equals(resolvedIndices2));
        assertTrue(resolvedIndices2 + " equals " + resolvedIndices1, resolvedIndices2.equals(resolvedIndices1));
        ResolvedIndices resolvedIndices3 = resolvedIndices2.withRemoteIndices(
            Map.of("remote2", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN))
        );
        assertNotEquals(resolvedIndices1.hashCode(), resolvedIndices3.hashCode());
        assertFalse(resolvedIndices1.equals(resolvedIndices3));
        assertFalse(resolvedIndices3.equals(resolvedIndices1));
        assertFalse(resolvedIndices1.equals(ResolvedIndices.unknown()));
        assertNotEquals(resolvedIndices1.hashCode(), ResolvedIndices.unknown().hashCode());
    }

    public void testHashCodeForConcreteIndices() {
        ResolvedIndices resolvedIndices1 = ResolvedIndices.of(ResolvedIndices.Local.Concrete.of(new Index("index", "index_uuid")))
            .withRemoteIndices(Map.of("remote", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN)))
            .withLocalSubActions(DeleteIndexAction.INSTANCE, ResolvedIndices.Local.of("other"));
        ResolvedIndices resolvedIndices2 = ResolvedIndices.of(ResolvedIndices.Local.Concrete.of(new Index("index", "index_uuid")))
            .withRemoteIndices(Map.of("remote", new OriginalIndices(new String[] { "remote_index" }, IndicesOptions.LENIENT_EXPAND_OPEN)))
            .withLocalSubActions(DeleteIndexAction.INSTANCE, ResolvedIndices.Local.of("other"));
        assertEquals(resolvedIndices1.hashCode(), resolvedIndices2.hashCode());
        assertTrue(resolvedIndices1 + " equals " + resolvedIndices2, resolvedIndices1.equals(resolvedIndices2));
        assertTrue(resolvedIndices2 + " equals " + resolvedIndices1, resolvedIndices2.equals(resolvedIndices1));
        ResolvedIndices resolvedIndices3 = resolvedIndices2.withLocalSubActions(
            DeleteIndexAction.INSTANCE,
            ResolvedIndices.Local.of("other2")
        );
        assertNotEquals(resolvedIndices1.hashCode(), resolvedIndices3.hashCode());
        assertFalse(resolvedIndices1.equals(resolvedIndices3));
        assertFalse(resolvedIndices3.equals(resolvedIndices1));
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
    }
}
