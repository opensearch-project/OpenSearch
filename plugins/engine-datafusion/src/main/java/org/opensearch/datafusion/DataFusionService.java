/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.datafusion.core.SessionContext;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for managing DataFusion contexts and operations - essentially like SearchService
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);

    // in memory contexts, similar to ReaderContext in SearchService, just a ptr to SessionContext for now.
    private final ConcurrentMapLong<SessionContext> contexts = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final AtomicLong idGenerator = new AtomicLong();

    @Override
    protected void doStart() {
        logger.info("Starting DataFusion service");
        try {
            // Test that the native library loads correctly
            String version = DataFusionJNI.getVersion();
            logger.info("DataFusion service started successfully. Version info: {}", version);
        } catch (Exception e) {
            logger.error("Failed to start DataFusion service", e);
            throw new RuntimeException("Failed to initialize DataFusion JNI", e);
        }
    }

    @Override
    protected void doStop() {
        logger.info("Stopping DataFusion service");
        // Close all named contexts
        for (SessionContext ctx : contexts.values()) {
            try {
                ctx.close();
            } catch (Exception e) {
                logger.warn("Error closing DataFusion context", e);
            }
        }
        contexts.clear();
        logger.info("DataFusion service stopped");
    }

    @Override
    protected void doClose() {
        // Ensure all resources are cleaned up
        doStop();
    }

    /**
     * Create a new named DataFusion context
     * @return the context ID
     */
    long createContext() {
        SessionContext ctx = new SessionContext();
        // just stores the context for now
        long id = idGenerator.incrementAndGet();
        SessionContext existing = contexts.put(id, ctx);
        assert existing == null;
        return id;
    }

    /**
     * Get a context by id
     * @param id the context id
     * @return the context ID, or null if not found
     */
    SessionContext getContext(long id) {
        return contexts.get(id);
    }

    /**
     * Close a context
     * @param contextId the context id
     * @return true if the context was found and closed, false otherwise
     */
    public boolean closeContext(long contextId) {
        try (SessionContext ignored = contexts.remove(contextId)) {
            // do nothing
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    /**
     * Get version information
     * @return JSON version string
     */
    public String getVersion() {
        return DataFusionJNI.getVersion();
    }
}
