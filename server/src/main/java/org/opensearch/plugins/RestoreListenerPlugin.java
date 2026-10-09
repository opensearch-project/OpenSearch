/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.snapshots.RestoreContext;

/**
 * An extension point for {@link Plugin}s that repair their own state after a snapshot restore, for example a saved
 * reference to an index that the restore did not bring back.
 * <p>
 * OpenSearch calls {@link #onRestore} on the cluster manager node once a finished restore is removed from the cluster
 * state. This is the same point at which a restore request that waits for completion returns. Each call runs on the
 * generic thread pool, inside {@link org.opensearch.identity.PluginSubject#runAs} of the plugin's own subject. This
 * interface extends {@link IdentityAwarePlugin} so that every plugin implementing it has a subject. Requests the plugin
 * makes from {@link #onRestore} therefore run as the plugin itself, never with the cluster manager's identity or with
 * none. The plugin must not stash the thread context to change that identity.
 * <p>
 * Delivery is best effort: a restore can be missed if the cluster manager fails right after removing it from the
 * cluster state, and an exception thrown by {@link #onRestore} is logged, not retried. State that must be correct even
 * then, or that can go wrong without any restore, needs a check where the plugin reads that state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface RestoreListenerPlugin extends IdentityAwarePlugin {

    /**
     * Called after a snapshot restore finishes, when at least one index had every shard restored.
     * <p>
     * May be called more than once for the same restore, and calls for different restores may run at the same time, so
     * it must be idempotent and thread-safe. The plugin decides whether the restore is relevant to it, and may look at the
     * current cluster state.
     *
     * @param context what the restore made available
     */
    void onRestore(RestoreContext context);
}
