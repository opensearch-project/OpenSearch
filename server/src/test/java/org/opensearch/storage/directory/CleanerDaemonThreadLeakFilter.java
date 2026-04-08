/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import com.carrotsearch.randomizedtesting.ThreadFilter;

import org.opensearch.index.store.remote.file.OnDemandBlockSnapshotIndexInput;

/**
 * The {@link java.lang.ref.Cleaner} instance used by {@link OnDemandBlockSnapshotIndexInput} creates
 * a daemon thread which is never stopped. This filter excludes that thread from leak detection.
 */
public final class CleanerDaemonThreadLeakFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return t.getName().startsWith(OnDemandBlockSnapshotIndexInput.CLEANER_THREAD_NAME_PREFIX);
    }
}
