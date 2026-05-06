/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;

/**
 * Backend-specific execution context produced by ShardScanInstructionHandler,
 * consumed by DatafusionSearcher at execute time.
 */
public record DataFusionSessionState(SessionContextHandle sessionContextHandle) implements BackendExecutionContext {
}
