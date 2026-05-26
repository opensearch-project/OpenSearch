/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineConfig;

/**
 * Initialization parameters for a {@link Committer}.
 *
 * <p>{@code preMergeCommitHook} is invoked by committers that own writers participating in
 * merges (e.g. the Lucene {@code MergeIndexWriter}) at the moment a merged segment becomes
 * ready but before it is made visible. The hook is expected to run on the merge thread
 * between {@code mergeMiddle} and {@code commitMerge}, while the underlying writer's
 * exclusive monitor is <em>not</em> held. The engine wires this hook to refresh-lock
 * acquisition so that merge-thread visibility is serialised against concurrent refreshes,
 * avoiding the lock inversion that would occur if the engine acquired the refresh lock
 * inside {@code commitMerge}. Any ownership acquired by the hook is transferred to the
 * engine's merge-apply callback, which releases it after the catalog is updated.
 *
 * <p>For merges that never reach the hook (pure Parquet merges, or Lucene merges that skip
 * because the shared writer has no matching segments), the merge-apply callback handles
 * coordination on its own. Committers that do not need this coordination may install the
 * hook but take no action when it fires.
 *
 * @param engineConfig         engine configuration
 * @param preMergeCommitHook   hook run on the merge thread before a merged segment is made
 *                             visible; ownership of anything it acquires is transferred to
 *                             the engine's merge-apply callback
 * @opensearch.experimental
 */
@ExperimentalApi
public record CommitterConfig(EngineConfig engineConfig, Runnable preMergeCommitHook, boolean isReplica) {

    public CommitterConfig(EngineConfig engineConfig, Runnable preMergeCommitHook) {
        this(engineConfig, preMergeCommitHook, false);
    }
}
