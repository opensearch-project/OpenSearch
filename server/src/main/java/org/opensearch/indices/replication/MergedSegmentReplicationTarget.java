/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.StepListener;
import org.opensearch.common.UUIDs;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationListener;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Represents the target of a merged segment replication event.
 *
 * @opensearch.internal
 */
public class MergedSegmentReplicationTarget extends AbstractSegmentReplicationTarget {
    public final static String MERGE_REPLICATION_PREFIX = "merge.";

    public MergedSegmentReplicationTarget(
        IndexShard indexShard,
        ReplicationCheckpoint checkpoint,
        SegmentReplicationSource source,
        ReplicationListener listener
    ) {
        super("merged_segment_replication_target", indexShard, checkpoint, source, listener);
    }

    @Override
    protected String getPrefix() {
        return MERGE_REPLICATION_PREFIX + UUIDs.randomBase64UUID() + ".";
    }

    @Override
    protected void getCheckpointMetadata(StepListener<CheckpointInfoResponse> checkpointInfoListener) {
        checkpointInfoListener.onResponse(new CheckpointInfoResponse(checkpoint, checkpoint.getMetadataMap(), null));
    }

    @Override
    protected void updateCheckpoint(ReplicationCheckpoint checkpoint, BiConsumer<ReplicationCheckpoint, IndexShard> checkpointUpdater) {}

    @Override
    protected void getFilesFromSource(
        CheckpointInfoResponse checkpointInfo,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetSegmentFilesResponse> getFilesListener
    ) {
        source.getMergedSegmentFiles(getId(), checkpoint, filesToFetch, indexShard, this::updateFileRecoveryBytes, getFilesListener);
    }

    @Override
    protected void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse) throws Exception {
        multiFileWriter.renameAllTempFiles();
    }

    @Override
    public MergedSegmentReplicationTarget retryCopy() {
        return new MergedSegmentReplicationTarget(indexShard, checkpoint, source, listener);
    }
}
