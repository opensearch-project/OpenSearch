/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.core.index.Index;

import java.util.List;

public class AssignOrdinalsClusterStateTaskExecutor implements ClusterStateTaskExecutor<AssignOrdinalsClusterStateTaskExecutor.Task> {

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> tasks) throws Exception {
        final Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        boolean changed = false;
        for (String index : currentState.getMetadata().getIndices().keySet()) {
            if (currentState.getMetadata().getIndices().get(index).isHasOrdinal()) {
                continue;
            }
            IndexMetadata indexMetadata = metadataBuilder.get(index);
            IndexMetadata.Builder indexmdBuilder = new IndexMetadata.Builder(indexMetadata);
            indexmdBuilder.compressedID(OrdinalGenerator.getInstance().nextOrdinal());
            indexmdBuilder.hasOrdinal(true);
            metadataBuilder.put(indexmdBuilder);
            changed = true;
        }
        if (!changed) {
            return ClusterTasksResult.<Task>builder().successes(tasks).build(currentState);
        }
        currentState = ClusterState.builder(currentState).metadata(metadataBuilder).build();
        final ClusterTasksResult.Builder<AssignOrdinalsClusterStateTaskExecutor.Task> resultBuilder = ClusterTasksResult.<
            AssignOrdinalsClusterStateTaskExecutor.Task>builder().successes(tasks);
        return resultBuilder.build(currentState);
    }

    public static class Task {

        private final Index index;
        private final IndexMetadata indexMetadata;

        public Task(final Index index, final IndexMetadata indexMetadata) {
            this.index = index;
            this.indexMetadata = indexMetadata;
        }

        public Index index() {
            return index;
        }

        public IndexMetadata indexMetadata() {
            return indexMetadata;
        }

        @Override
        public String toString() {
            return index.getName() + " (" + indexMetadata.getIndexUUID() + ")";
        }
    }
}
