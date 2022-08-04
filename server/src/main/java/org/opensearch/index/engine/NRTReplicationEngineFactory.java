/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Engine Factory implementation used with Segment Replication that wires up replica shards with an ${@link NRTReplicationEngine}
 * and primary with an ${@link InternalEngine}
 *
 * @opensearch.internal
 */
public class NRTReplicationEngineFactory implements EngineFactory {

    private static final Logger logger = LogManager.getLogger(NRTReplicationEngineFactory.class);

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        Engine engine;
        if (config.isReadOnlyReplica()) {
            if (config.getIndexSettings().isRemoteStoreEnabled()) {
                engine = new NRTReplicationNoOpEngine(config);
            } else {
                engine = new NRTReplicationEngine(config);
            }
        } else {
            engine = new InternalEngine(config);
        }
        logger.info("EngineUsed={}", engine.getClass().getSimpleName());
        return engine;
    }
}
