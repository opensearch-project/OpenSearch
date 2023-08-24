/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class NoOpCryptoProviderTests extends OpenSearchTestCase {

    public void testInitEncryptionMetadata() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        Object encryptionMetadata = cryptoProvider.initEncryptionMetadata();
        assertNotNull(encryptionMetadata);
    }

    public void testAdjustContentSizeForPartialEncryption() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        long originalSize = 1000L;
        long adjustedSize = cryptoProvider.adjustContentSizeForPartialEncryption(new Object(), originalSize);
        assertEquals(originalSize, adjustedSize);
    }

    public void testEstimateEncryptedLengthOfEntireContent() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        long originalSize = 2000L;
        long estimatedSize = cryptoProvider.estimateEncryptedLengthOfEntireContent(new Object(), originalSize);
        assertEquals(originalSize, estimatedSize);
    }

    public void testEstimateDecryptedLength() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        long originalSize = 1500L;
        long estimatedSize = cryptoProvider.estimateDecryptedLength(new Object(), originalSize);
        assertEquals(originalSize, estimatedSize);
    }

    public void testCreateEncryptingStream() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        InputStreamContainer inputStream = randomStream();
        InputStreamContainer encryptedStream = cryptoProvider.createEncryptingStream(new Object(), inputStream);
        assertEquals(inputStream, encryptedStream);
    }

    public void testCreateEncryptingStreamOfPart() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        InputStreamContainer inputStream = randomStream();
        InputStreamContainer encryptedStream = cryptoProvider.createEncryptingStreamOfPart(new Object(), inputStream, 2, 1);
        assertEquals(inputStream, encryptedStream);
    }

    private InputStreamContainer randomStream() {
        byte[] bytes = randomAlphaOfLength(10).getBytes();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        int offset = randomIntBetween(0, bytes.length - 1);
        return new InputStreamContainer(byteArrayInputStream, bytes.length, offset);
    }

    public void testLoadEncryptionMetadata() throws IOException {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        EncryptedHeaderContentSupplier supplier = (start, length) -> { throw new UnsupportedOperationException("Not implemented"); };
        Object encryptionMetadata = cryptoProvider.loadEncryptionMetadata(supplier);
        assertNotNull(encryptionMetadata);
    }

    public void testCreateDecryptingStream() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        InputStream encryptedStream = randomStream().getInputStream();
        InputStream decryptedStream = cryptoProvider.createDecryptingStream(encryptedStream);
        assertEquals(encryptedStream, decryptedStream);
    }

    public void testCreateDecryptingStreamOfRange() {
        NoOpCryptoProvider cryptoProvider = new NoOpCryptoProvider();
        Object cryptoContext = new Object();
        long startPos = 0L;
        long endPos = 100L;
        DecryptedRangedStreamProvider streamProvider = cryptoProvider.createDecryptingStreamOfRange(cryptoContext, startPos, endPos);
        assertNotNull(streamProvider);
        InputStream stream = randomStream().getInputStream();
        InputStream decryptedStream = streamProvider.getDecryptedStreamProvider().apply(stream); // Replace with your encrypted input stream
        assertEquals(stream, decryptedStream);
    }
}
