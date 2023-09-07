/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.frame;

import org.opensearch.common.blobstore.transfer.stream.OffsetRangeFileInputStream;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.encryption.MockKeyProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.ParsedCiphertext;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import org.mockito.Mockito;

public class CryptoTests extends OpenSearchTestCase {

    private static FrameCryptoHandler frameCryptoHandler;

    private static FrameCryptoHandler frameCryptoHandlerTrailingAlgo;

    static class CustomFrameCryptoHandlerTest extends FrameCryptoHandler {
        private final int frameSize;

        CustomFrameCryptoHandlerTest(AwsCrypto awsCrypto, HashMap<String, String> config, int frameSize) {
            super(awsCrypto, config, () -> {});
            this.frameSize = frameSize;
        }

        @Override
        public int getFrameSize() {
            return frameSize;
        }
    }

    @Before
    public void setupResources() {
        frameCryptoHandler = new CustomFrameCryptoHandlerTest(
            createAwsCrypto(CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256),
            new HashMap<>(),
            100
        );
        frameCryptoHandlerTrailingAlgo = new CustomFrameCryptoHandlerTest(
            createAwsCrypto(CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384),
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

    static class EncryptedStoreTest {
        byte[] encryptedContent;
        int encryptedLength;
        long rawLength;
        byte[] rawContent;
    }

    private EncryptedStoreTest verifyAndGetEncryptedContent() throws IOException, URISyntaxException {
        return verifyAndGetEncryptedContent(false, frameCryptoHandler);
    }

    private EncryptedStoreTest verifyAndGetEncryptedContent(boolean truncateRemainderPart, FrameCryptoHandler frameCryptoHandler)
        throws IOException, URISyntaxException {

        long maxLength = 50 * 1024;
        long rawContentLength = truncateRemainderPart
            ? maxLength / frameCryptoHandler.getFrameSize() * frameCryptoHandler.getFrameSize()
            : maxLength;
        byte[] rawContent = randomAlphaOfLength((int) rawContentLength).getBytes(StandardCharsets.UTF_8);

        EncryptionMetadata cryptoContext = frameCryptoHandler.initEncryptionMetadata();

        int encLength = 0;
        byte[] encryptedContent = new byte[100 * 1024];
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(rawContent, 0, rawContent.length)) {

            InputStreamContainer stream = new InputStreamContainer(inputStream, rawContent.length, 0);
            InputStreamContainer encInputStream = frameCryptoHandler.createEncryptingStream(cryptoContext, stream);
            assertNotNull(encInputStream);

            int readBytes;
            while ((readBytes = encInputStream.getInputStream().read(encryptedContent, encLength, 1024)) != -1) {
                encLength += readBytes;
            }
        }

        long calculatedEncryptedLength = frameCryptoHandler.estimateEncryptedLengthOfEntireContent(cryptoContext, rawContentLength);
        assertEquals(encLength, calculatedEncryptedLength);

        EncryptedStoreTest encryptedStoreTest = new EncryptedStoreTest();
        encryptedStoreTest.encryptedLength = encLength;
        encryptedStoreTest.encryptedContent = encryptedContent;
        encryptedStoreTest.rawLength = rawContentLength;
        encryptedStoreTest.rawContent = rawContent;
        return encryptedStoreTest;
    }

    public void testEncryptedDecryptedLengthEstimations() {
        // Testing for 100 iterations
        for (int i = 0; i < 100; i++) {
            // Raw content size cannot be max value as encrypted size will overflow for the same.
            long n = randomLongBetween(0, Integer.MAX_VALUE / 2);
            FrameCryptoHandler frameCryptoHandler = new CustomFrameCryptoHandlerTest(
                createAwsCrypto(CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA384_ECDSA_P384),
                new HashMap<>(),
                randomIntBetween(10, 10240)
            );
            EncryptionMetadata cryptoContext = frameCryptoHandler.initEncryptionMetadata();
            long encryptedLength = frameCryptoHandler.estimateEncryptedLengthOfEntireContent(cryptoContext, n);
            ParsedCiphertext parsedCiphertext = new ParsedCiphertext(cryptoContext.getCiphertextHeaderBytes());
            long decryptedLength = frameCryptoHandler.estimateDecryptedLength(parsedCiphertext, encryptedLength);
            assertEquals(n, decryptedLength);
        }
    }

    public void testSingleStreamEncryption() throws IOException, URISyntaxException {

        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStoreTest.encryptedContent,
            0,
            encryptedStoreTest.encryptedLength
        );
        long decryptedRawBytes = decryptAndVerify(byteArrayInputStream, encryptedStoreTest.encryptedLength, encryptedStoreTest.rawContent);
        assertEquals(encryptedStoreTest.rawLength, decryptedRawBytes);
    }

    public void testSingleStreamEncryptionTrailingSignatureAlgo() throws IOException, URISyntaxException {

        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent(false, frameCryptoHandlerTrailingAlgo);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStoreTest.encryptedContent,
            0,
            encryptedStoreTest.encryptedLength
        );
        long decryptedRawBytes = decryptAndVerify(byteArrayInputStream, encryptedStoreTest.encryptedLength, encryptedStoreTest.rawContent);
        assertEquals(encryptedStoreTest.rawLength, decryptedRawBytes);
    }

    public void testDecryptionOfCorruptedContent() throws IOException, URISyntaxException {

        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        encryptedStoreTest.encryptedContent = "Corrupted content".getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStoreTest.encryptedContent,
            0,
            encryptedStoreTest.encryptedLength
        );

        Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStoreTest.encryptedLength, encryptedStoreTest.rawContent)
        );

    }

    private long decryptAndVerify(InputStream encryptedStream, long encSize, byte[] rawContent) throws IOException {
        long totalRawBytes = 0;
        try (
            ByteArrayInputStream fis = new ByteArrayInputStream(rawContent);
            InputStream decryptingStream = frameCryptoHandler.createDecryptingStream(encryptedStream)
        ) {
            byte[] decryptedBuffer = new byte[1024];
            byte[] actualBuffer = new byte[1024];
            int readActualBytes;
            int readBytes;
            while ((readBytes = decryptingStream.read(decryptedBuffer, 0, decryptedBuffer.length)) != -1) {
                readActualBytes = fis.read(actualBuffer, 0, readBytes);
                assertEquals(readActualBytes, readBytes);
                assertArrayEquals(actualBuffer, decryptedBuffer);
                totalRawBytes += readActualBytes;
            }
            assertEquals(rawContent.length, totalRawBytes);
        }
        return totalRawBytes;
    }

    public void testMultiPartStreamsEncryption() throws IOException, URISyntaxException {
        EncryptionMetadata encryptionMetadata = frameCryptoHandler.initEncryptionMetadata();

        long rawContentLength = 50 * 1024;
        byte[] rawContent = randomAlphaOfLength((int) rawContentLength).getBytes(StandardCharsets.UTF_8);
        assertEquals(rawContentLength, rawContent.length);

        Path path = createTempFile();
        File file = path.toFile();
        Files.write(path, rawContent);
        assertEquals(rawContentLength, Files.size(path));

        byte[] encryptedContent = new byte[1024 * 100];
        int parts;
        long partSize, lastPartSize;
        partSize = getPartSize(rawContentLength, frameCryptoHandler.getFrameSize());
        parts = numberOfParts(rawContentLength, partSize);
        lastPartSize = rawContentLength - (partSize * (parts - 1));

        int encLength = 0;
        for (int partNo = 0; partNo < parts; partNo++) {
            long size = partNo == parts - 1 ? lastPartSize : partSize;
            long pos = partNo * partSize;
            try (InputStream inputStream = getMultiPartStreamSupplier(file).apply(size, pos)) {
                InputStreamContainer rawStream = new InputStreamContainer(inputStream, size, pos);
                InputStreamContainer encStream = frameCryptoHandler.createEncryptingStreamOfPart(
                    encryptionMetadata,
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
        encLength += frameCryptoHandler.getTrailingSignatureLength(encryptionMetadata);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(encryptedContent, 0, encLength);
        decryptAndVerify(byteArrayInputStream, encLength, rawContent);

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
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        validateBlockDownload(encryptedStoreTest, 0, (int) encryptedStoreTest.rawLength - 1);
    }

    public void testBlockBasedDecryptionForEntireFileWithLinedUpFrameAlongFileBoundary() throws Exception {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent(true, frameCryptoHandler);
        assertEquals(
            "This test is meant for file size exactly divisible by frame size",
            0,
            (encryptedStoreTest.rawLength % frameCryptoHandler.getFrameSize())
        );
        validateBlockDownload(encryptedStoreTest, 0, (int) encryptedStoreTest.rawLength - 1);
    }

    public void testCorruptedTrailingSignature() throws IOException, URISyntaxException {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent(false, frameCryptoHandlerTrailingAlgo);
        byte[] trailingData = "corrupted".getBytes(StandardCharsets.UTF_8);
        byte[] corruptedTrailingContent = Arrays.copyOf(
            encryptedStoreTest.encryptedContent,
            encryptedStoreTest.encryptedContent.length + trailingData.length
        );
        System.arraycopy(trailingData, 0, corruptedTrailingContent, encryptedStoreTest.encryptedContent.length, trailingData.length);
        encryptedStoreTest.encryptedContent = corruptedTrailingContent;
        encryptedStoreTest.encryptedLength = corruptedTrailingContent.length;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStoreTest.encryptedContent,
            0,
            encryptedStoreTest.encryptedLength
        );
        BadCiphertextException ex = Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStoreTest.encryptedLength, encryptedStoreTest.rawContent)
        );
        Assert.assertEquals("Bad trailing signature", ex.getMessage());
    }

    public void testNoTrailingSignatureForTrailingAlgo() throws IOException, URISyntaxException {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent(false, frameCryptoHandlerTrailingAlgo);
        EncryptionMetadata cryptoContext = frameCryptoHandlerTrailingAlgo.initEncryptionMetadata();
        int trailingLength = frameCryptoHandler.getTrailingSignatureLength(cryptoContext);
        byte[] removedTrailingContent = Arrays.copyOf(
            encryptedStoreTest.encryptedContent,
            encryptedStoreTest.encryptedContent.length - trailingLength
        );
        encryptedStoreTest.encryptedContent = removedTrailingContent;
        encryptedStoreTest.encryptedLength = removedTrailingContent.length;
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
            encryptedStoreTest.encryptedContent,
            0,
            encryptedStoreTest.encryptedLength
        );
        BadCiphertextException ex = Assert.assertThrows(
            BadCiphertextException.class,
            () -> decryptAndVerify(byteArrayInputStream, encryptedStoreTest.encryptedLength, encryptedStoreTest.rawContent)
        );
        Assert.assertEquals("Bad trailing signature", ex.getMessage());
    }

    public void testOutputSizeEstimateWhenHandlerIsNull() {
        CryptoMaterialsManager cryptoMaterialsManager = Mockito.mock(CryptoMaterialsManager.class);
        DecryptionHandler<?> decryptionHandler = DecryptionHandler.create(cryptoMaterialsManager);
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
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        int maxBlockNum = (int) encryptedStoreTest.rawLength / frameCryptoHandler.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockDownload(
            encryptedStoreTest,
            randomIntBetween(5, maxBlockNum / 2) * frameCryptoHandler.getFrameSize(),
            randomIntBetween(maxBlockNum / 2 + 1, maxBlockNum) * frameCryptoHandler.getFrameSize() - 1
        );
    }

    public void testRandomRangeDecryption() throws Exception {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        // Testing for 100 iterations
        for (int testIteration = 0; testIteration < 100; testIteration++) {
            int startPos = randomIntBetween(0, (int) encryptedStoreTest.rawLength - 1);
            int endPos = randomIntBetween(startPos, (int) encryptedStoreTest.rawLength - 1);
            validateBlockDownload(encryptedStoreTest, startPos, endPos);
        }
    }

    public void testDecryptionWithSameStartEndPos() throws Exception {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        int pos = randomIntBetween(0, (int) encryptedStoreTest.rawLength - 1);
        for (int testIteration = 0; testIteration < frameCryptoHandler.getFrameSize(); testIteration++) {
            validateBlockDownload(encryptedStoreTest, pos, pos);
        }
    }

    public void testBlockBasedDecryptionForLastBlock() throws Exception {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        int maxBlockNum = (int) encryptedStoreTest.rawLength / frameCryptoHandler.getFrameSize();
        assert maxBlockNum > 5;
        validateBlockDownload(
            encryptedStoreTest,
            randomIntBetween(1, maxBlockNum - 1) * frameCryptoHandler.getFrameSize(),
            (int) encryptedStoreTest.rawLength - 1
        );
    }

    private void validateBlockDownload(EncryptedStoreTest encryptedStoreTest, int startPos, int endPos) throws Exception {

        EncryptedHeaderContentSupplier encryptedHeaderContentSupplier = createEncryptedHeaderContentSupplier(
            encryptedStoreTest.encryptedContent
        );
        ParsedCiphertext cryptoContext = frameCryptoHandler.loadEncryptionMetadata(encryptedHeaderContentSupplier);
        DecryptedRangedStreamProvider decryptedStreamProvider = frameCryptoHandler.createDecryptingStreamOfRange(
            cryptoContext,
            startPos,
            endPos
        );

        long[] transformedRange = decryptedStreamProvider.getAdjustedRange();
        int encryptedBlockSize = (int) (transformedRange[1] - transformedRange[0] + 1);
        byte[] encryptedBlockBytes = new byte[encryptedBlockSize];
        System.arraycopy(encryptedStoreTest.encryptedContent, (int) transformedRange[0], encryptedBlockBytes, 0, encryptedBlockSize);
        ByteArrayInputStream encryptedStream = new ByteArrayInputStream(encryptedBlockBytes, 0, encryptedBlockSize);
        InputStream decryptingStream = decryptedStreamProvider.getDecryptedStreamProvider().apply(encryptedStream);

        try (ByteArrayInputStream rawStream = new ByteArrayInputStream(encryptedStoreTest.rawContent, startPos, endPos - startPos + 1)) {
            decryptAndVerifyBlock(decryptingStream, rawStream, startPos, endPos);
        }
    }

    public void testBlockBasedDecryptionForFirstBlock() throws Exception {
        EncryptedStoreTest encryptedStoreTest = verifyAndGetEncryptedContent();
        // All block requests should properly line up with frames otherwise decryption will fail due to partial frames.
        int blockEnd = randomIntBetween(5, (int) encryptedStoreTest.rawLength / frameCryptoHandler.getFrameSize()) * frameCryptoHandler
            .getFrameSize() - 1;
        validateBlockDownload(encryptedStoreTest, 0, blockEnd);
    }

    private long decryptAndVerifyBlock(
        InputStream decryptedStream,
        ByteArrayInputStream rawStream,
        int rawContentStartPos,
        int rawContentEndPos
    ) throws IOException {
        long totalRawBytes = 0;

        byte[] decryptedBuffer = new byte[100];
        byte[] actualBuffer = new byte[100];

        int readActualBytes;
        int readBytes;
        while ((readBytes = decryptedStream.read(decryptedBuffer, 0, decryptedBuffer.length)) != -1) {
            readActualBytes = rawStream.read(actualBuffer, 0, Math.min(actualBuffer.length, readBytes));
            assertEquals(readActualBytes, readBytes);
            assertArrayEquals(actualBuffer, decryptedBuffer);
            totalRawBytes += readActualBytes;
        }
        assertEquals(rawContentEndPos - rawContentStartPos + 1, totalRawBytes);
        return totalRawBytes;
    }

    public void testEmptyContentCrypto() throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {});
        EncryptionMetadata cryptoContext = frameCryptoHandler.initEncryptionMetadata();
        InputStreamContainer stream = new InputStreamContainer(byteArrayInputStream, 0, 0);
        InputStreamContainer encryptingStream = frameCryptoHandler.createEncryptingStream(cryptoContext, stream);
        InputStream decryptingStream = frameCryptoHandler.createDecryptingStream(encryptingStream.getInputStream());
        decryptingStream.readAllBytes();
    }

    public void testEmptyContentCryptoTrailingSignatureAlgo() throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[] {});
        EncryptionMetadata cryptoContext = frameCryptoHandlerTrailingAlgo.initEncryptionMetadata();
        InputStreamContainer stream = new InputStreamContainer(byteArrayInputStream, 0, 0);
        InputStreamContainer encryptingStream = frameCryptoHandlerTrailingAlgo.createEncryptingStream(cryptoContext, stream);
        InputStream decryptingStream = frameCryptoHandlerTrailingAlgo.createDecryptingStream(encryptingStream.getInputStream());
        decryptingStream.readAllBytes();
    }

}
