/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.shard.IndexShard;

import java.util.Objects;

/**
 * Publish primary shard referenced segments.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class ReferencedSegmentsPublisher {
    private final PublishAction publishAction;

    // This Component is behind feature flag so we are manually binding this in IndicesModule.
    @Inject
    public ReferencedSegmentsPublisher(PublishReferencedSegmentsAction publishAction) {
        this(publishAction::publish);
    }

    public ReferencedSegmentsPublisher(PublishAction publishAction) {
        this.publishAction = Objects.requireNonNull(publishAction);
    }

    public void publish(IndexShard indexShard, ReferencedSegmentsCheckpoint checkpoint) {
        publishAction.publish(indexShard, checkpoint);
    }

    /**
     * Represents an action that is invoked to publish referenced segments checkpoint to replica shard
     *
     * @opensearch.api
     */
    @ExperimentalApi
    public interface PublishAction {
        void publish(IndexShard indexShard, ReferencedSegmentsCheckpoint checkpoint);
    }

    /**
     * NoOp Checkpoint publisher
     */
    public static final ReferencedSegmentsPublisher EMPTY = new ReferencedSegmentsPublisher((indexShard, checkpoint) -> {});
}
