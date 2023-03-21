/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.store.RemoteDirectory;

/**
 * A {@code RemoteBufferedOutputDirectory} is an extension of Remote Directory where creation of new files are allowed.
 * A remoteDirectory contains only files (no sub-folder hierarchy).
 *
 * @opensearch.internal
 */
public class RemoteBufferedOutputDirectory extends RemoteDirectory {
    public RemoteBufferedOutputDirectory(BlobContainer blobContainer) {
        super(blobContainer);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return new RemoteBufferedIndexOutput(name, this.blobContainer);
    }
}
