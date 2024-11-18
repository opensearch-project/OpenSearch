/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

public abstract class IngestionConsumerFactory<T extends IngestionShardConsumer> {
    protected IngestionSourceConfig config;

    public void initialize(IngestionSourceConfig config) {
        this.config = config;
    }


    public abstract T createShardConsumer(String clientId, int shardId);

}
