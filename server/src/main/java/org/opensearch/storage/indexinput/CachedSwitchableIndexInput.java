/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CachedIndexInput implementation that wraps a SwitchableIndexInput for use with FileCache.
 * Constructor will accept FileCache, fileName, localDirectory, remoteDirectory, transferManager,
 * cacheFromRemote flag, threadPool, and tieredStoragePrefetchSettingsSupplier.
 * getIndexInput, length, isClosed, and close will be added in the implementation PR.
 */
public class CachedSwitchableIndexInput implements CachedIndexInput {

    private final SwitchableIndexInput switchableIndexInput;
    private final AtomicBoolean isClosed;

    // Placeholder constructor. Real constructor will be added in the implementation PR.
    CachedSwitchableIndexInput() {
        this.switchableIndexInput = null;
        this.isClosed = new AtomicBoolean(false);
    }

    @Override
    public IndexInput getIndexInput() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
