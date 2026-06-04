/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * The {@link java.lang.ref.Cleaner} instance used by {@link OnDemandBlockSnapshotIndexInput} creates
 * a daemon thread which is never stopped, nor do we have a handle to stop it. This filter
 * excludes that thread from the leak detection logic.
 */
public final class CleanerDaemonThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith(OnDemandBlockSnapshotIndexInput.CLEANER_THREAD_NAME_PREFIX);
    }
}
