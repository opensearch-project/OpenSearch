/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;

/**
 * The interface that defines how {@link Translog.Snapshot} will get replayed into the Engine
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface TranslogRecoveryRunner {

    /**
     * Recovers a translog snapshot
     * @param snapshot the snapshot of translog operations
     * @return recoveredOps
     * @throws IOException exception while recovering operations
     */
    int run(Translog.Snapshot snapshot) throws IOException;
}
