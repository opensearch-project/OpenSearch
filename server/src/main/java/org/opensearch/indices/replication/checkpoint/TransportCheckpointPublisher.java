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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.client.Client;
import org.opensearch.indices.replication.copy.ReplicationCheckpoint;

public class TransportCheckpointPublisher {

    protected static Logger logger = LogManager.getLogger(TransportCheckpointPublisher.class);

    private final Client client;

    public TransportCheckpointPublisher(Client client) {
        this.client = client;
    }

    public void publish(ReplicationCheckpoint checkpoint) {
        logger.trace("Publishing Checkpoint {}", checkpoint);
        client.admin().indices().publishCheckpoint(new PublishCheckpointRequest(checkpoint), new ActionListener<RefreshResponse>() {
            @Override
            public void onResponse(RefreshResponse response) {
                logger.trace("Successfully published checkpoints");
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Publishing Checkpoints from primary to replicas failed", e);
            }
        });
    }
}
