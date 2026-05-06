/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.be.datafusion.WireConfigSnapshot;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Immutable configuration record for creating a native SessionContext via
 * {@link NativeBridge#createSessionContext(SessionContextConfig)}.
 *
 * @param readerPtr   pointer to the native DataFusion reader (shard view)
 * @param runtimePtr  pointer to the native DataFusion runtime
 * @param tableName   logical table name to register in the session context
 * @param contextId   query/task context identifier (0 if none)
 * @param queryConfig query config snapshot to pass to native
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record SessionContextConfig(long readerPtr, long runtimePtr, String tableName, long contextId, WireConfigSnapshot queryConfig) {}
