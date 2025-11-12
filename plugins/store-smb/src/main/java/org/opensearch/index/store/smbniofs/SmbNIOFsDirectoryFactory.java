/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.smbniofs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.SmbDirectoryWrapper;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory to create a new NIO File System type directory accessible as a SMB share
 */
public final class SmbNIOFsDirectoryFactory extends FsDirectoryFactory {

    @Override
    public Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        return new SmbDirectoryWrapper(new NIOFSDirectory(location, lockFactory));
    }
}
