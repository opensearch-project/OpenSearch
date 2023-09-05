/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IOContext;
import org.junit.Before;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class RemoteBufferedOutputDirectoryTests extends OpenSearchTestCase {

    private BlobContainer blobContainer;
    private RemoteBufferedOutputDirectory remoteBufferedOutputDirectory;

    @Before
    public void setup() {
        blobContainer = mock(BlobContainer.class);
        remoteBufferedOutputDirectory = new RemoteBufferedOutputDirectory(blobContainer);
    }

    public void testCreateOutput() {
        String testBlobName = "testBlob";
        assert (remoteBufferedOutputDirectory.createOutput(testBlobName, IOContext.DEFAULT) instanceof RemoteBufferedIndexOutput);

    }
}
