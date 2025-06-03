/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.shard.IndexShard;

import java.util.Objects;

/**
 * Publish merged segment.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class MergedSegmentPublisher {
    protected static Logger logger = LogManager.getLogger(MergedSegmentPublisher.class);

    private final PublishAction publishAction;

    // This Component is behind feature flag so we are manually binding this in IndicesModule.
    @Inject
    public MergedSegmentPublisher(PublishMergedSegmentAction publishAction) {
        this(publishAction::publish);
    }

    public MergedSegmentPublisher(PublishAction publishAction) {
        this.publishAction = Objects.requireNonNull(publishAction);
    }

    public void publish(IndexShard indexShard, ReplicationSegmentCheckpoint checkpoint) {
        publishAction.publish(indexShard, checkpoint);
    }

    /**
     * Represents an action that is invoked to publish merged segment to replica shard
     *
     * @opensearch.api
     */
    @ExperimentalApi
    public interface PublishAction {
        void publish(IndexShard indexShard, ReplicationSegmentCheckpoint checkpoint);
    }

    /**
     * NoOp Checkpoint publisher
     */
    public static final MergedSegmentPublisher EMPTY = new MergedSegmentPublisher((indexShard, checkpoint) -> {});
}
