/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart.mocks;

import org.opensearch.OpenSearchException;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.nio.file.Path;

public class MockFsBlobStore extends FsBlobStore {

    private final boolean triggerDataIntegrityFailure;

    public MockFsBlobStore(int bufferSizeInBytes, Path path, boolean readonly, boolean triggerDataIntegrityFailure) throws IOException {
        super(bufferSizeInBytes, path, readonly);
        this.triggerDataIntegrityFailure = triggerDataIntegrityFailure;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new MockFsVerifyingBlobContainer(this, path, buildAndCreate(path), triggerDataIntegrityFailure);
        } catch (IOException ex) {
            throw new OpenSearchException("failed to create blob container", ex);
        }
    }
}
