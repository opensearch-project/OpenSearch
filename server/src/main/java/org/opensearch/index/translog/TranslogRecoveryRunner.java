/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * The interface that defines how {@link Translog.Snapshot} will get replayed into the Engine
 *
 * @opensearch.api
 */
@FunctionalInterface
@PublicApi(since = "1.0.0")
public interface TranslogRecoveryRunner {

    /**
     * Recovers a translog snapshot
     * @param snapshot the snapshot of translog operations
     * @return recoveredOps
     * @throws IOException exception while recovering operations
     */
    int run(Translog.Snapshot snapshot) throws IOException;
}
