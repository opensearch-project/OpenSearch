/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.frame;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.ParsedCiphertext;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import com.amazonaws.encryptionsdk.internal.SignaturePolicy;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.encryption.MockKeyProvider;
import org.opensearch.encryption.OffsetRangeFileInputStream;
import org.opensearch.encryption.frame.core.AwsCrypto;
import org.opensearch.encryption.frame.core.DecryptionHandler;
import org.opensearch.encryption.frame.core.EncryptionMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class CryptoTests extends OpenSearchTestCase {

    private static FrameCryptoProvider frameCryptoProvider;

    private static FrameCryptoProvider frameCryptoProviderTrailingAlgo;

    static class CustomFrameCryptoProviderTest extends FrameCryptoProvider {
        private final int frameSize;

        CustomFrameCryptoProviderTest(AwsCrypto awsCrypto, HashMap<String, String> config, int frameSize) {
            super(awsCrypto, config);
            this.frameSize = frameSize;
        }

        @Override
        public int getFrameSize() {
            return frameSize;
        }
    }

    @Before
    public void setupResources() {
        frameCryptoProvider = new CustomFrameCryptoProviderTest(
            createAwsCrypto(CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY),
            new HashMap<>(),
            100
        );
        frameCryptoProviderTrailingAlgo = new CustomFrameCryptoProviderTest(
            createAwsCrypto(CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY_ECDSA_P384),
            new HashMap<>(),
            100
        );
    }

    private AwsCrypto createAwsCrypto(CryptoAlgorithm cryptoAlgorithm) {
        MockKeyProvider keyProvider = new MockKeyProvider();
        CachingCryptoMaterialsManager cachingMaterialsManager = CachingCryptoMaterialsManager.newBuilder()
            .withMasterKeyProvider(keyProvider)
            .withCache(new LocalCryptoMaterialsCache(1000))
            .withMaxAge(10, TimeUnit.MINUTES)
            .build();

        return new AwsCrypto(cachingMaterialsManager, cryptoAlgorithm);
    }

    static class EncryptedStore {
        byte[] encryptedContent;
        long rawLength;
        int encryptedLength;
        File file;
    }

    private EncryptedStore verifyAndGetEncryptedContent() throws IOException, URISyntaxException {
        return verifyAndGetEncryptedContent(false, frameCryptoProvider);
    }

    private EncryptedStore verifyAndGetEncryptedContent(boolean truncateRemainderPart, FrameCryptoProvider frameCryptoProvider)
        throws IOException, URISyntaxException {
        String path = CryptoTests.class.getResource("/raw_content_for_crypto_test").toURI().getPath();
        File file = new File(path);

        Object cryptoContext = frameCryptoProvider.initEncryptionMetadata();
        long length;
        byte[] encryptedContent = new byte[1024 * 20];
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            FileChannel channel = fileInputStream.getChannel();
            length = truncateRemainderPart ? channel.size() - (channel.size() % frameCryptoProvider.getFrameSize()) : channel.size();
        }

        int encLength = 0;
        try (OffsetRangeFileInputStream inputStream = new OffsetRangeFileInputStream(file.toPath(), length, 0)) {

            InputStreamContainer stream = new InputStreamContainer(inputStream, length, 0);
            InputStreamContainer encInputStream = frameCryptoProvider.createEncryptingStream(cryptoContext, stream);
            assertNotNull(encInputStream);

            int readBytes;
            while ((readBytes = encInputStream.getInputStream().read(encryptedContent, encLength, 1024)) != -1) {
                encLength += readBytes;
            }
        }

        long calculatedEncryptedLength = frameCryptoProvider.estimateEncryptedLengthOfEntireContent(cryptoContext, length);
        assertEquals(encLength, calculatedEncryptedLength);

        EncryptedStore encryptedStore = new EncryptedStore();
        encryptedStore.encryptedLength = encLength;
        encryptedStore.encryptedContent = encryptedContent;
        encryptedStore.rawLength = length;
        encryptedStore.file = file;
        return encryptedStore;
    }

    public void testEncryptedDecryptedLengthEstimations() {
        // Testing for 100 iterations
        for (int i = 0; i < 100; i++) {
            // Raw content size cannot be max value as encrypted size will overflow for the same.
            long n = randomLongBetween(0, Integer.MAX_VALUE / 2);
            FrameCryptoProvider frameCryptoProvider = new CustomFrameCryptoProviderTest(
                createAwsCrypto(CryptoAlgorithm.ALG_AES_256_GCM_HKDF_SHA512_COMMIT_KEY),
                new HashMap<>(),
                randomIntBetween(10, 10240)
            );
            EncryptionMetadata cryptoContext = (EncryptionMetadata) frameCryptoProvider.initEncryptionMetadata();
            long encryptedLength = frameCryptoProvider.estimateEncryptedLengthOfEntireContent(cryptoContext, n);
            ParsedCiphertext parsedCiphertext = new ParsedCiphertext(cryptoContext.getCiphertextHeaderBytes());
            long decryptedLength = frameCryptoProvider.estimateDecryptedLength(parsedCiphertext, encryptedLength);
            assertEquals(n, decryptedLength);
        }
    }

    public void testSingleStreamEncryption() throws IOException, URISyntaxException {

        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        long decryptedRawBytes = decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file);
        assertEquals(encryptedStore.rawLength, decryptedRawBytes);
    }

    public void testSingleStreamEncryptionTrailingSignatureAlgo() throws IOException, URISyntaxException {

        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(false, frameCryptoProviderTrailingAlgo);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        long decryptedRawBytes = decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file);
        assertEquals(encryptedStore.rawLength, decryptedRawBytes);
    }

    public void testDecryptionOfCorruptedContent() throws IOException, URISyntaxException {

        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        encryptedStore.encryptedContent = "Corrupted content".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );

        Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file)
        );

    }

    private long decryptAndVerify(InputStream encryptedStream, long encSize, File file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        long totalRawBytes = 0;
        try (FileChannel channel = inputStream.getChannel()) {
            channel.position(0);

            InputStream decryptingStream = frameCryptoProvider.createDecryptingStream(encryptedStream);
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] decryptedBuffer = new byte[1024];
                byte[] actualBuffer = new byte[1024];
                int readActualBytes;
                int readBytes;
                while ((readBytes = decryptingStream.read(decryptedBuffer, 0, decryptedBuffer.length)) != -1) {
                    readActualBytes = fis.read(actualBuffer, 0, actualBuffer.length);
                    assertEquals(readActualBytes, readBytes);
                    assertArrayEquals(actualBuffer, decryptedBuffer);
                    totalRawBytes += readActualBytes;
                }
            }
        }
        return totalRawBytes;
    }

    public void testMultiPartStreamsEncryption() throws IOException, URISyntaxException {
        Object cryptoContextObj = frameCryptoProvider.initEncryptionMetadata();
        EncryptionMetadata encryptionMetadata = (EncryptionMetadata) cryptoContextObj;
        String path = CryptoTests.class.getResource("/raw_content_for_crypto_test").toURI().getPath();
        File file = new File(path);
        byte[] encryptedContent = new byte[1024 * 20];
        int parts;
        long partSize, lastPartSize;
        long length;
        try (FileInputStream inputStream = new FileInputStream(file); FileChannel channel = inputStream.getChannel()) {
            length = channel.size();
        }
        partSize = getPartSize(length, frameCryptoProvider.getFrameSize());
        parts = numberOfParts(length, partSize);
        lastPartSize = length - (partSize * (parts - 1));

        int encLength = 0;
        for (int partNo = 0; partNo < parts; partNo++) {
            long size = partNo == parts - 1 ? lastPartSize : partSize;
            long pos = partNo * partSize;
            try (InputStream inputStream = getMultiPartStreamSupplier(file).apply(size, pos)) {
                InputStreamContainer rawStream = new InputStreamContainer(inputStream, size, pos);
                InputStreamContainer encStream = frameCryptoProvider.createEncryptingStreamOfPart(
                    cryptoContextObj,
                    rawStream,
                    parts,
                    partNo
                );
                int readBytes;
                int curEncryptedBytes = 0;
                while ((readBytes = encStream.getInputStream().read(encryptedContent, encLength, 1024)) != -1) {
                    encLength += readBytes;
                    curEncryptedBytes += readBytes;
                }
                assertEquals(encStream.getContentLength(), curEncryptedBytes);
            }
        }
        encLength += frameCryptoProvider.getTrailingSignatureLength(encryptionMetadata);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(encryptedContent, 0, encLength);
        decryptAndVerify(byteArrayInputStream, encLength, file);

    }

    private long getPartSize(long contentLength, int frameSize) {

        double optimalPartSizeDecimal = (double) contentLength / randomIntBetween(5, 10);
        // round up so we don't push the upload over the maximum number of parts
        long optimalPartSize = (long) Math.ceil(optimalPartSizeDecimal);
        if (optimalPartSize < frameSize) {
            optimalPartSize = frameSize;
        }

        if (optimalPartSize >= contentLength) {
            return contentLength;
        }

        if (optimalPartSize % frameSize > 0) {
            // When using encryption, parts must line up correctly along cipher block boundaries
            optimalPartSize = optimalPartSize - (optimalPartSize % frameSize) + frameSize;
        }
        return optimalPartSize;
    }

    private int numberOfParts(final long totalSize, final long partSize) {
        if (totalSize % partSize == 0) {
            return (int) (totalSize / partSize);
        }
        return (int) (totalSize / partSize) + 1;
    }

    private BiFunction<Long, Long, CheckedInputStream> getMultiPartStreamSupplier(File localFile) {
        return (size, position) -> {
            OffsetRangeFileInputStream offsetRangeInputStream;
            try {
                offsetRangeInputStream = new OffsetRangeFileInputStream(localFile.toPath(), size, position);
            } catch (IOException e) {
                return null;
            }
            return new CheckedInputStream(offsetRangeInputStream, new CRC32());
        };
    }

    public void testBlockBasedDecryptionForEntireFile() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        assertTrue(
            "This test is meant for file size not exactly divisible by frame size",
            (encryptedStore.rawLength & frameCryptoProvider.getFrameSize()) != 0
        );
        validateBlockDownload(encryptedStore, 0, (int) encryptedStore.rawLength - 1);
    }

    public void testBlockBasedDecryptionForEntireFileWithLinedUpFrameAlongFileBoundary() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(true, frameCryptoProvider);
        assertEquals(
            "This test is meant for file size exactly divisible by frame size",
            0,
            (encryptedStore.rawLength % frameCryptoProvider.getFrameSize())
        );
        validateBlockDownload(encryptedStore, 0, (int) encryptedStore.rawLength - 1);
    }

    public void testCorruptedTrailingSignature() throws IOException, URISyntaxException {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(false, frameCryptoProviderTrailingAlgo);
        byte[] trailingData = "corrupted".getBytes(StandardCharsets.UTF_8);
        byte[] corruptedTrailingContent = Arrays.copyOf(
            encryptedStore.encryptedContent,
            encryptedStore.encryptedContent.length + trailingData.length
        );
        System.arraycopy(trailingData, 0, corruptedTrailingContent, encryptedStore.encryptedContent.length, trailingData.length);
        encryptedStore.encryptedContent = corruptedTrailingContent;
        encryptedStore.encryptedLength = corruptedTrailingContent.length;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        BadCiphertextException ex = Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file)
        );
        Assert.assertEquals("Bad trailing signature", ex.getMessage());
    }

    public void testNoTrailingSignatureForTrailingAlgo() throws IOException, URISyntaxException {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent(false, frameCryptoProviderTrailingAlgo);
        Object cryptoContext = frameCryptoProviderTrailingAlgo.initEncryptionMetadata();
        int trailingLength = frameCryptoProvider.getTrailingSignatureLength(cryptoContext);
        byte[] removedTrailingContent = Arrays.copyOf(
            encryptedStore.encryptedContent,
            encryptedStore.encryptedContent.length - trailingLength
        );
        encryptedStore.encryptedContent = removedTrailingContent;
        encryptedStore.encryptedLength = removedTrailingContent.length;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStore.encryptedContent,
            0,
            encryptedStore.encryptedLength
        );
        BadCiphertextException ex = Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStore.encryptedLength, encryptedStore.file)
        );
        Assert.assertEquals("Bad trailing signature", ex.getMessage());
    }

    public void testOutputSizeEstimateWhenHandlerIsNull() {
        CryptoMaterialsManager cryptoMaterialsManager = Mockito.mock(CryptoMaterialsManager.class);
        DecryptionHandler<?> decryptionHandler = DecryptionHandler.create(
            cryptoMaterialsManager,
            CommitmentPolicy.RequireEncryptRequireDecrypt,
            SignaturePolicy.AllowEncryptAllowDecrypt,
            1
        );
        int inputLen = 50;
        int len = decryptionHandler.estimateOutputSize(inputLen);
        assertEquals(inputLen, len);
    }

    private EncryptedHeaderContentSupplier createEncryptedHeaderContentSupplier(byte[] encryptedContent) {
        return (start, end) -> {
            int len = (int) (end - start + 1);
            byte[] bytes = new byte[len];
            System.arraycopy(encryptedContent, (int) start, bytes, (int) start, len);
            return bytes;
        };
    }

    public void testBlockBasedDecryptionForMiddleBlock() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        int maxBlockNum = (int) encryptedStore.rawLength / frameCryptoProvider.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockDownload(
            encryptedStore,
            randomIntBetween(5, maxBlockNum / 2) * frameCryptoProvider.getFrameSize(),
            randomIntBetween(maxBlockNum / 2 + 1, maxBlockNum) * frameCryptoProvider.getFrameSize() - 1
        );
    }

    public void testRandomRangeDecryption() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        // Testing for 100 iterations
        for (int testIteration = 0; testIteration < 100; testIteration++) {
            int startPos = randomIntBetween(0, (int) encryptedStore.rawLength - 1);
            int endPos = randomIntBetween(startPos, (int) encryptedStore.rawLength - 1);
            validateBlockDownload(encryptedStore, startPos, endPos);
        }
    }

    public void testDecryptionWithSameStartEndPos() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        int pos = randomIntBetween(0, (int) encryptedStore.rawLength - 1);
        for (int testIteration = 0; testIteration < frameCryptoProvider.getFrameSize(); testIteration++) {
            validateBlockDownload(encryptedStore, pos, pos);
        }
    }

    public void testBlockBasedDecryptionForLastBlock() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        int maxBlockNum = (int) encryptedStore.rawLength / frameCryptoProvider.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockDownload(
            encryptedStore,
            randomIntBetween(1, maxBlockNum - 1) * frameCryptoProvider.getFrameSize(),
            (int) encryptedStore.rawLength - 1
        );
    }

    private void validateBlockDownload(EncryptedStore encryptedStore, int startPos, int endPos) throws Exception {

        EncryptedHeaderContentSupplier encryptedHeaderContentSupplier = createEncryptedHeaderContentSupplier(
            encryptedStore.encryptedContent
        );
        Object cryptoContext = frameCryptoProvider.loadEncryptionMetadata(encryptedHeaderContentSupplier);
        DecryptedRangedStreamProvider decryptedStreamProvider = frameCryptoProvider.createDecryptingStreamOfRange(
            cryptoContext,
            startPos,
            endPos
        );

        long[] transformedRange = decryptedStreamProvider.getAdjustedRange();
        int encryptedBlockSize = (int) (transformedRange[1] - transformedRange[0] + 1);
        byte[] encryptedBlockBytes = new byte[encryptedBlockSize];
        System.arraycopy(encryptedStore.encryptedContent, (int) transformedRange[0], encryptedBlockBytes, 0, encryptedBlockSize);
        ByteArrayInputStream encryptedStream = new ByteArrayInputStream(encryptedBlockBytes, 0, encryptedBlockSize);
        InputStream decryptingStream = decryptedStreamProvider.getDecryptedStreamProvider().apply(encryptedStream);

        decryptAndVerifyBlock(decryptingStream, encryptedStore.file, startPos, endPos);
    }

    public void testBlockBasedDecryptionForFirstBlock() throws Exception {
        EncryptedStore encryptedStore = verifyAndGetEncryptedContent();
        // All block requests should properly line up with frames otherwise decryption will fail due to partial frames.
        int blockEnd = randomIntBetween(5, (int) encryptedStore.rawLength / frameCryptoProvider.getFrameSize()) * frameCryptoProvider
            .getFrameSize() - 1;
        validateBlockDownload(encryptedStore, 0, blockEnd);
    }

    private long decryptAndVerifyBlock(InputStream decryptedStream, File file, int rawContentStartPos, int rawContentEndPos)
        throws IOException {
        long totalRawBytes = 0;

        try (FileInputStream fis = new FileInputStream(file); FileChannel channel = fis.getChannel()) {
            channel.position(rawContentStartPos);
            byte[] decryptedBuffer = new byte[100];
            byte[] actualBuffer = new byte[100];

            int readActualBytes;
            int readBytes;
            while ((readBytes = decryptedStream.read(decryptedBuffer, 0, decryptedBuffer.length)) != -1) {
                readActualBytes = fis.read(actualBuffer, 0, Math.min(actualBuffer.length, rawContentEndPos - rawContentStartPos + 1));
                rawContentEndPos -= readActualBytes;
                assertEquals(readActualBytes, readBytes);
                assertArrayEquals(actualBuffer, decryptedBuffer);
                totalRawBytes += readActualBytes;
            }
        }
        return totalRawBytes;
    }

    public void testEmptyContentCrypto() throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {});
        Object cryptoContext = frameCryptoProvider.initEncryptionMetadata();
        InputStreamContainer stream = new InputStreamContainer(byteArrayInputStream, 0, 0);
        InputStreamContainer encryptingStream = frameCryptoProvider.createEncryptingStream(cryptoContext, stream);
        InputStream decryptingStream = frameCryptoProvider.createDecryptingStream(encryptingStream.getInputStream());
        decryptingStream.readAllBytes();
    }

    public void testEmptyContentCryptoTrailingSignatureAlgo() throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {});
        Object cryptoContext = frameCryptoProviderTrailingAlgo.initEncryptionMetadata();
        InputStreamContainer stream = new InputStreamContainer(byteArrayInputStream, 0, 0);
        InputStreamContainer encryptingStream = frameCryptoProviderTrailingAlgo.createEncryptingStream(cryptoContext, stream);
        InputStream decryptingStream = frameCryptoProviderTrailingAlgo.createDecryptingStream(encryptingStream.getInputStream());
        decryptingStream.readAllBytes();
    }

}
