/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.Version;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexAction;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.indexaware.IndexAwareSearchRequestScope.isInternalByDefault;

public class IndexAwareSearchRequestScopeTests extends OpenSearchTestCase {

    public void testOrdinarySearchIsNotInternal() {
        assertFalse(isInternalByDefault(null, List.of(ordinaryIndex("my-index"))));
    }

    public void testNoTargetIndicesIsNotInternal() {
        assertFalse(isInternalByDefault(null, Collections.emptyList()));
    }

    public void testOrdinaryParentTaskIsNotInternal() {
        Task ordinaryParent = new Task(1, "transport", "indices:data/read/search", "desc", TaskId.EMPTY_TASK_ID, Map.of());
        assertFalse(isInternalByDefault(ordinaryParent, List.of(ordinaryIndex("my-index"))));
    }

    // Each by-query action below builds its parent task the way the action itself does, to confirm that
    // all three really produce a BulkByScrollTask.

    public void testDeleteByQueryParentTaskIsInternal() {
        Task parentTask = new DeleteByQueryRequest("my-index").createTask(
            1,
            "transport",
            DeleteByQueryAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        assertTrue(isInternalByDefault(parentTask, List.of(ordinaryIndex("my-index"))));
    }

    public void testUpdateByQueryParentTaskIsInternal() {
        Task parentTask = new UpdateByQueryRequest("my-index").createTask(
            1,
            "transport",
            UpdateByQueryAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        assertTrue(isInternalByDefault(parentTask, List.of(ordinaryIndex("my-index"))));
    }

    public void testReindexParentTaskIsInternal() {
        Task parentTask = new ReindexRequest().createTask(1, "transport", ReindexAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
        assertTrue(isInternalByDefault(parentTask, List.of(ordinaryIndex("my-index"))));
    }

    public void testSystemIndexTargetIsInternal() {
        assertTrue(isInternalByDefault(null, List.of(systemIndex(".my-system-index"))));
    }

    public void testMixOfOrdinaryAndSystemIndexTargetsIsInternal() {
        assertTrue(isInternalByDefault(null, List.of(ordinaryIndex("my-index"), systemIndex(".my-system-index"))));
    }

    public void testSearchWithoutRemoteIndicesHasNoRemoteIndices() {
        assertFalse(IndexAwareSearchRequestScope.hasRemoteIndices(Map.of()));
    }

    // The check takes no ccs_minimize_roundtrips argument on purpose: a remote index excludes the search
    // in either mode, so there is a single path to test here.
    public void testSearchWithARemoteIndexHasRemoteIndices() {
        assertTrue(IndexAwareSearchRequestScope.hasRemoteIndices(remoteIndices("remote-index")));
    }

    private static Map<String, OriginalIndices> remoteIndices(String... indices) {
        return Map.of("my-remote-cluster", new OriginalIndices(indices, IndicesOptions.LENIENT_EXPAND_OPEN));
    }

    private static IndexMetadata ordinaryIndex(String name) {
        return IndexMetadata.builder(name).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
    }

    private static IndexMetadata systemIndex(String name) {
        return IndexMetadata.builder(name).settings(settings(Version.CURRENT)).system(true).numberOfShards(1).numberOfReplicas(0).build();
    }
}
