/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.CachedIndexInput;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.storage.utils.DirectoryUtils.getFilePath;
import static org.opensearch.storage.utils.DirectoryUtils.getFilePathSwitchable;

public class CachedSwitchableIndexInput implements CachedIndexInput {

    private final SwitchableIndexInput switchableIndexInput;
    private final AtomicBoolean isClosed;

    public CachedSwitchableIndexInput(
        FileCache fileCache,
        String fileName,
        FSDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        TransferManager transferManager,
        boolean cacheFromRemote,
        ThreadPool threadPool
    ) throws IOException {
        isClosed = new AtomicBoolean(false);
        String resourceDescription = "SwitchableIndexInput (path=" + getFilePath(localDirectory, fileName) + ")";
        switchableIndexInput = new SwitchableIndexInput(resourceDescription, fileName, getFilePath(localDirectory, fileName), getFilePathSwitchable(localDirectory, fileName), fileCache, localDirectory, remoteDirectory, transferManager, cacheFromRemote, threadPool);
    }

    @Override
    public IndexInput getIndexInput() {
        if (isClosed.get()) throw new AlreadyClosedException("Index input is already closed");
        return switchableIndexInput;
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void close() throws Exception {
        if (!isClosed.getAndSet(true)) {
            switchableIndexInput.close();
        }
    }
}
