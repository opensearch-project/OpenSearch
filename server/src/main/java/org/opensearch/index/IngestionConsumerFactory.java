/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import java.util.Map;

public interface IngestionConsumerFactory<T extends IngestionShardConsumer,P extends IngestionShardPointer> {
    void initialize(Map<String, Object> params);

    T createShardConsumer(String clientId, int shardId);

    P parsePointerFromString(String pointer);
}
