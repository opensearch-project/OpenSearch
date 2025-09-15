/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.Objects;

/**
 * Engine Factory implementation used with streaming ingestion.
 */
public class IngestionEngineFactory implements EngineFactory {

    private final IngestionConsumerFactory ingestionConsumerFactory;

    public IngestionEngineFactory(IngestionConsumerFactory ingestionConsumerFactory) {
        this.ingestionConsumerFactory = Objects.requireNonNull(ingestionConsumerFactory);
    }

    /**
     * Document replication equivalent in pull-based ingestion is to ingest on both primary and replica nodes using the
     * IngestionEngine. Segment replication will use the NRTReplicationEngine on replicas.
     */
    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        boolean isDocRep = getReplicationType(config) == ReplicationType.DOCUMENT;

        // NRTReplicationEngine is used for segment replication on replicas
        if (isDocRep == false && config.isReadOnlyReplica()) {
            return new NRTReplicationEngine(config);
        }

        IngestionEngine ingestionEngine = new IngestionEngine(config, ingestionConsumerFactory);
        ingestionEngine.start();
        return ingestionEngine;
    }

    private ReplicationType getReplicationType(EngineConfig config) {
        IndexSettings indexSettings = config.getIndexSettings();
        return indexSettings.getValue(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING);
    }
}
