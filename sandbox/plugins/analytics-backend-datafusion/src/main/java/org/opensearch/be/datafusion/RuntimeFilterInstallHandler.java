/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.RuntimeFilterInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;

/**
 * Installs a join runtime filter on the fragment's DataFusion session.
 *
 * <p>The fragment's Substrait plan references the filter by id through an
 * {@code os_runtime_filter(id, key)} predicate; this handler supplies the bytes that id resolves
 * to. It must therefore run after the scan-setup handler that created the session, and before the
 * fragment executes.
 *
 * <p>Never fails the fragment. A missing session, a malformed payload, or a native refusal all
 * leave the filter uninstalled, and the UDF answers {@code true} for an id it cannot find — so the
 * fragment runs unfiltered and produces the same rows, just more of them into the shuffle. That is
 * the whole reason the feature can be enabled without risking results.
 */
public class RuntimeFilterInstallHandler implements FragmentInstructionHandler<RuntimeFilterInstructionNode> {

    private static final Logger LOGGER = LogManager.getLogger(RuntimeFilterInstallHandler.class);

    @Override
    public BackendExecutionContext apply(
        RuntimeFilterInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        if (!(backendContext instanceof DataFusionSessionState sessionState)) {
            // Unlike the broadcast handler, this does not throw: a runtime filter arriving
            // without a session is a wiring mistake that must not cost a correct query.
            LOGGER.warn(
                "[runtime-filter] filter {} skipped: expected a DataFusion session from a prior handler, got {}",
                node.getFilterId(),
                backendContext == null ? "null" : backendContext.getClass().getSimpleName()
            );
            return backendContext;
        }

        byte[] payload = node.getPayload();
        try {
            long status = NativeBridge.installRuntimeFilter(sessionState.sessionContextHandle().getPointer(), node.getFilterId(), payload);
            if (status == NativeBridge.RUNTIME_FILTER_INSTALLED) {
                LOGGER.debug("[runtime-filter] installed filter {} ({} bytes)", node.getFilterId(), payload == null ? 0 : payload.length);
            } else {
                LOGGER.warn("[runtime-filter] filter {} not installed by the backend; fragment runs unfiltered", node.getFilterId());
            }
        } catch (Exception e) {
            LOGGER.warn("[runtime-filter] filter " + node.getFilterId() + " install failed; fragment runs unfiltered", e);
        }
        return backendContext;
    }
}
