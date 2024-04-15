/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;

public class BatchTestUtil {
    public static List<ShardId> setUpShards(int numberOfShards) {
        List<ShardId> shards = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            ShardId shardId = new ShardId("test", "_na_", shardNumber);
            shards.add(shardId);
        }
        return shards;
    }
}
