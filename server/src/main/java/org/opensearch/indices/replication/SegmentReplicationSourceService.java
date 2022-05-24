/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

/**
 * Service responsible for handling replication requests from replica shards. By default the "Source" is the primary
 * shard, but it can be any shard in a replication group.
 *
 * @opensearch.internal
 */
public class SegmentReplicationSourceService {

    /**
     * Actions related to segment replication that this component will handle.
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/segrep/checkpoint_info";
        public static final String GET_FILES = "internal:index/shard/segrep/get_files";
    }

    public SegmentReplicationSourceService() {
        // TODO add handlers for both actions.
    }
}
