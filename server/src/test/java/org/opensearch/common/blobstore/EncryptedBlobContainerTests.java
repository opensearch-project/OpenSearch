
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    @SuppressWarnings("unchecked")
    public void testWriteBlobWithMetadataWithoutCryptoMetadata() throws IOException {
        // Setup mocks
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        // Setup crypto context
        Object cryptoContext = new Object();
        when(cryptoHandler.initEncryptionMetadata()).thenReturn(cryptoContext);

        // Setup encrypted stream
        InputStream encryptedInputStream = new ByteArrayInputStream("encrypted".getBytes(StandardCharsets.UTF_8));
        InputStreamContainer encryptedStreamContainer = new InputStreamContainer(encryptedInputStream, 100, 0);
        when(cryptoHandler.createEncryptingStream(eq(cryptoContext), any(InputStreamContainer.class))).thenReturn(encryptedStreamContainer);

        // Setup encrypted length estimation
        when(cryptoHandler.estimateEncryptedLengthOfEntireContent(eq(cryptoContext), anyLong())).thenReturn(100L);

        // Setup underlying container to accept write
        doNothing().when(blobContainer)
            .writeBlobWithMetadata(any(String.class), any(InputStream.class), anyLong(), eq(true), any(Map.class));

        // Test data
        String blobName = "test-blob";
        InputStream inputStream = new ByteArrayInputStream("test data".getBytes(StandardCharsets.UTF_8));
        long blobSize = 9L;
        Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");

        // Execute
        encryptedBlobContainer.writeBlobWithMetadata(blobName, inputStream, blobSize, true, metadata);

        // Verify encryption was initialized
        verify(cryptoHandler).initEncryptionMetadata();

        // Verify underlying container was called with encrypted stream
        verify(blobContainer).writeBlobWithMetadata(eq(blobName), any(InputStream.class), eq(100L), eq(true), eq(metadata));
    }

    @SuppressWarnings("unchecked")
    public void testWriteBlobWithMetadataWithCryptoMetadata() throws IOException {
        // Setup mocks
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        // Setup crypto context
        Object cryptoContext = new Object();
        when(cryptoHandler.initEncryptionMetadata()).thenReturn(cryptoContext);

        // Setup encrypted stream
        InputStream encryptedInputStream = new ByteArrayInputStream("encrypted".getBytes(StandardCharsets.UTF_8));
        InputStreamContainer encryptedStreamContainer = new InputStreamContainer(encryptedInputStream, 120, 0);
        when(cryptoHandler.createEncryptingStream(eq(cryptoContext), any(InputStreamContainer.class))).thenReturn(encryptedStreamContainer);

        // Setup encrypted length estimation
        when(cryptoHandler.estimateEncryptedLengthOfEntireContent(eq(cryptoContext), anyLong())).thenReturn(120L);

        // Setup underlying container
        doNothing().when(blobContainer)
            .writeBlobWithMetadata(
                any(String.class),
                any(InputStream.class),
                anyLong(),
                eq(false),
                any(Map.class),
                any(CryptoMetadata.class)
            );

        // Test data
        String blobName = "test-blob-with-crypto";
        InputStream inputStream = new ByteArrayInputStream("test data with crypto".getBytes(StandardCharsets.UTF_8));
        long blobSize = 21L;
        Map<String, String> metadata = new HashMap<>();
        metadata.put("index", "my-index");

        // Create CryptoMetadata
        Settings kmsSettings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-east-1:123:key/test-key")
            .put("kms.encryption_context", "tenant=acme")
            .build();
        CryptoMetadata cryptoMetadata = new CryptoMetadata("test-provider", "aws-kms", kmsSettings);

        // Execute
        encryptedBlobContainer.writeBlobWithMetadata(blobName, inputStream, blobSize, false, metadata, cryptoMetadata);

        // Verify encryption was initialized
        verify(cryptoHandler).initEncryptionMetadata();

        // Verify underlying container was called with encrypted stream and CryptoMetadata
        verify(blobContainer).writeBlobWithMetadata(
            eq(blobName),
            any(InputStream.class),
            eq(120L),
            eq(false),
            eq(metadata),
            eq(cryptoMetadata)
        );
    }

    @SuppressWarnings("unchecked")
    public void testWriteBlobWithMetadataDelegatesCryptoMetadata() throws IOException {
        // Setup mocks
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        // Setup crypto context
        Object cryptoContext = new Object();
        when(cryptoHandler.initEncryptionMetadata()).thenReturn(cryptoContext);

        // Setup encrypted stream
        InputStreamContainer encryptedStreamContainer = new InputStreamContainer(new ByteArrayInputStream(new byte[50]), 50, 0);
        when(cryptoHandler.createEncryptingStream(eq(cryptoContext), any(InputStreamContainer.class))).thenReturn(encryptedStreamContainer);
        when(cryptoHandler.estimateEncryptedLengthOfEntireContent(eq(cryptoContext), anyLong())).thenReturn(50L);

        // Setup ArgumentCaptor to capture the CryptoMetadata passed to underlying container
        ArgumentCaptor<CryptoMetadata> cryptoMetadataCaptor = ArgumentCaptor.forClass(CryptoMetadata.class);
        doNothing().when(blobContainer)
            .writeBlobWithMetadata(
                any(String.class),
                any(InputStream.class),
                anyLong(),
                eq(true),
                any(Map.class),
                cryptoMetadataCaptor.capture()
            );

        // Create CryptoMetadata with specific settings
        Settings kmsSettings = Settings.builder()
            .put("kms.key_arn", "arn:aws:kms:us-west-2:456:key/delegation-test-key")
            .put("kms.encryption_context", "env=prod,service=search")
            .build();
        CryptoMetadata inputCryptoMetadata = new CryptoMetadata("delegate-provider", "aws-kms", kmsSettings);

        // Execute
        encryptedBlobContainer.writeBlobWithMetadata(
            "delegation-test",
            new ByteArrayInputStream(new byte[10]),
            10L,
            true,
            new HashMap<>(),
            inputCryptoMetadata
        );

        // Verify the exact same CryptoMetadata instance was passed through
        CryptoMetadata capturedCryptoMetadata = cryptoMetadataCaptor.getValue();
        assertSame("CryptoMetadata should be passed through unchanged", inputCryptoMetadata, capturedCryptoMetadata);
        assertEquals("delegate-provider", capturedCryptoMetadata.keyProviderName());
        assertEquals("aws-kms", capturedCryptoMetadata.keyProviderType());
        assertEquals("arn:aws:kms:us-west-2:456:key/delegation-test-key", capturedCryptoMetadata.settings().get("kms.key_arn"));
    }

    @SuppressWarnings("unchecked")
    public void testWriteBlobWithMetadataEncryptsCorrectly() throws IOException {
        // Setup mocks
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        // Setup crypto context
        Object cryptoContext = new Object();
        when(cryptoHandler.initEncryptionMetadata()).thenReturn(cryptoContext);

        // Capture the InputStreamContainer passed to createEncryptingStream
        ArgumentCaptor<InputStreamContainer> inputStreamCaptor = ArgumentCaptor.forClass(InputStreamContainer.class);

        // Setup encrypted stream with larger size (simulating encryption overhead)
        long originalSize = 100L;
        long encryptedSize = 128L; // Encryption adds overhead
        InputStreamContainer encryptedStreamContainer = new InputStreamContainer(
            new ByteArrayInputStream(new byte[(int) encryptedSize]),
            encryptedSize,
            0
        );
        when(cryptoHandler.createEncryptingStream(eq(cryptoContext), inputStreamCaptor.capture())).thenReturn(encryptedStreamContainer);
        when(cryptoHandler.estimateEncryptedLengthOfEntireContent(cryptoContext, originalSize)).thenReturn(encryptedSize);

        // Capture the stream and size passed to underlying container
        ArgumentCaptor<Long> sizeCaptor = ArgumentCaptor.forClass(Long.class);
        doNothing().when(blobContainer)
            .writeBlobWithMetadata(any(String.class), any(InputStream.class), sizeCaptor.capture(), eq(true), any(Map.class));

        // Execute with original data
        byte[] originalData = new byte[(int) originalSize];
        InputStream inputStream = new ByteArrayInputStream(originalData);
        encryptedBlobContainer.writeBlobWithMetadata("encrypt-test", inputStream, originalSize, true, new HashMap<>());

        // Verify the original stream container was passed to encryption
        InputStreamContainer capturedInput = inputStreamCaptor.getValue();
        assertEquals("Original size should be passed to encryption", originalSize, capturedInput.getContentLength());

        // Verify the encrypted size was passed to underlying container
        assertEquals("Encrypted size should be passed to underlying container", Long.valueOf(encryptedSize), sizeCaptor.getValue());

        // Verify encryption length estimation was called
        verify(cryptoHandler).estimateEncryptedLengthOfEntireContent(cryptoContext, originalSize);
    }

    @SuppressWarnings("unchecked")
    public void testListBlobsByPrefixInSortedOrder() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        BlobMetadata blob1 = new PlainBlobMetadata("blob1", 100);
        BlobMetadata blob2 = new PlainBlobMetadata("blob2", 200);
        List<BlobMetadata> mockBlobs = List.of(blob1, blob2);

        when(blobContainer.listBlobsByPrefixInSortedOrder("prefix", 10, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC)).thenReturn(
            mockBlobs
        );

        List<BlobMetadata> result = encryptedBlobContainer.listBlobsByPrefixInSortedOrder(
            "prefix",
            10,
            BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
        );

        assertEquals(2, result.size());
        assertTrue(result.get(0) instanceof EncryptedBlobMetadata);
        assertTrue(result.get(1) instanceof EncryptedBlobMetadata);
    }

    @SuppressWarnings("unchecked")
    public void testListBlobsByPrefixInSortedOrderReturnsNull() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        when(blobContainer.listBlobsByPrefixInSortedOrder("prefix", 10, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC)).thenReturn(null);

        List<BlobMetadata> result = encryptedBlobContainer.listBlobsByPrefixInSortedOrder(
            "prefix",
            10,
            BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
        );

        assertNull(result);
    }

    @SuppressWarnings("unchecked")
    public void testEncryptedBlobContainerDoesNotCallListBlobsByPrefix() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class);
        CryptoHandler<Object, Object> cryptoHandler = mock(CryptoHandler.class);
        EncryptedBlobContainer<Object, Object> encryptedBlobContainer = new EncryptedBlobContainer<>(blobContainer, cryptoHandler);

        BlobMetadata blob1 = new PlainBlobMetadata("blob1", 100);
        List<BlobMetadata> mockBlobs = List.of(blob1);

        when(blobContainer.listBlobsByPrefixInSortedOrder("prefix", 5, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC)).thenReturn(
            mockBlobs
        );

        encryptedBlobContainer.listBlobsByPrefixInSortedOrder("prefix", 5, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);

        verify(blobContainer).listBlobsByPrefixInSortedOrder("prefix", 5, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

}
