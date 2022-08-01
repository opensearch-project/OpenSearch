/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication
 * is enabled.  This Engine does not create an IndexWriter, rather it refreshes a {@link NRTReplicationReaderManager}
 * with new Segments when received from an external source.
 *
 * @opensearch.internal
 */
public class NRTNoOpReplicationEngine extends NRTReplicationEngine {

    public NRTNoOpReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }
}
