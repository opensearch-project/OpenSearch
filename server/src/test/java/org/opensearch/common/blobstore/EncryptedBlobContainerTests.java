
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptedBlobContainerTests extends OpenSearchTestCase {

    public void testBlobContainerReadBlobWithMetadata() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer encryptedBlobContainer = new EncryptedBlobContainer(blobContainer, cryptoHandler);
        InputStreamWithMetadata inputStreamWithMetadata = new InputStreamWithMetadata(
            new ByteArrayInputStream(new byte[0]),
            new HashMap<>()
        );
        when(blobContainer.readBlobWithMetadata("test")).thenReturn(inputStreamWithMetadata);
        InputStream decrypt = new ByteArrayInputStream(new byte[2]);
        when(cryptoHandler.createDecryptingStream(inputStreamWithMetadata.getInputStream())).thenReturn(decrypt);
        InputStreamWithMetadata result = encryptedBlobContainer.readBlobWithMetadata("test");
        assertEquals(result.getInputStream(), decrypt);
        assertEquals(result.getMetadata(), inputStreamWithMetadata.getMetadata());
    }

}
