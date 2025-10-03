/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.seqno.SeqNoStats;

@PublicApi(since = "1.0.0")
public interface CheckpointState {

    /**
     * @return the persisted local checkpoint for this Engine
     */
    long getPersistedLocalCheckpoint();

    /**
     * @return the latest checkpoint that has been processed but not necessarily persisted.
     * Also see {@link #getPersistedLocalCheckpoint()}
     */
    long getProcessedLocalCheckpoint();

    /**
     * @return a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     */
    SeqNoStats getSeqNoStats(long globalCheckpoint);

    /**
     * Returns the latest global checkpoint value that has been persisted in the underlying storage (i.e. translog's checkpoint)
     */
    long getLastSyncedGlobalCheckpoint();

    long getMinRetainedSeqNo();
}
