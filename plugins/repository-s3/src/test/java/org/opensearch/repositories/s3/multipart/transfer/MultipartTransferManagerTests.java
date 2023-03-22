/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.multipart.transfer;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import org.opensearch.common.Stream;
import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.OffsetRangeFileInputStream;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MultipartTransferManagerTests extends OpenSearchTestCase {

    private AmazonS3 s3;
    private MultipartTransferManager multipartTransferManager;
    private ExecutorService priorityRemoteUpload;
    private ExecutorService remoteUpload;
    private final List<InputStream> openInputStreams = new ArrayList<>();

    public void setUp() throws Exception {
        s3 = mock(AmazonS3.class);

        priorityRemoteUpload = Executors.newSingleThreadExecutor();
        remoteUpload = Executors.newSingleThreadExecutor();
        multipartTransferManager = new MultipartTransferManager(
            TransferManagerBuilder.standard().withS3Client(s3),
            priorityRemoteUpload,
            remoteUpload
        );
        super.setUp();
    }

    private Path setupFile(int fileSize) throws IOException {
        Path testFile = createTempFile();
        Files.write(testFile, randomByteArrayOfLength(fileSize), StandardOpenOption.APPEND);
        return testFile;
    }

    private InputStream openInputStreamToFile(Path testFile, long offset, long size) throws IOException {
        InputStream inputStream = new OffsetRangeFileInputStream(testFile, size, offset);
        openInputStreams.add(inputStream);
        return inputStream;
    }

    private Supplier<Stream> createStreamSupplier(Path testFile, long offset, long partSize) throws IOException {
        InputStream inputStream = openInputStreamToFile(testFile, offset, partSize);
        return () -> new Stream(
            inputStream,
            partSize,
            offset
        );
    }

    private List<Supplier<Stream>> getStreamSuppliers(final Path testFile, final long partSize, final long totalContentLength) throws IOException {
        List<Supplier<Stream>> streamSuppliers = new ArrayList<>();
        long offset = 0;
        int nParts = 0;
        long remainingLength = totalContentLength;
        while (remainingLength > partSize) {
            streamSuppliers.add(createStreamSupplier(testFile, offset, partSize));
            offset += partSize;
            remainingLength -= partSize;
            nParts++;
        }
        long lastPartSize = partSize;
        if (remainingLength > 0) {
            lastPartSize = remainingLength;
            streamSuppliers.add(createStreamSupplier(testFile, offset, partSize));
            nParts++;
        }

        assert (nParts - 1) * partSize + lastPartSize == totalContentLength : "expected part sizes to add up to total content length";

        return streamSuppliers;
    }

    public void testUploadInSingleChunk() throws IOException, InterruptedException {
        PutObjectResult putObjectResult = new PutObjectResult();
        putObjectResult.setETag("eTag");
        putObjectResult.setVersionId("versionId");
        when(s3.putObject(any(PutObjectRequest.class))).thenReturn(putObjectResult);

        int testFileSizeInBytes = 128;
        Path testFile = setupFile(testFileSizeInBytes);
        StreamContext streamContext = new StreamContext(getStreamSuppliers(testFile, testFileSizeInBytes, testFileSizeInBytes), testFileSizeInBytes);

        Upload upload = multipartTransferManager.upload(
            "sample_bucket",
            "sample_file_name",
            null,
            null,
            streamContext,
            null,
            WritePriority.HIGH
        );

        upload.waitForCompletion();

        verify(s3, times(1)).putObject(any(PutObjectRequest.class));
    }

    public void testMultipartUpload() throws IOException, InterruptedException {
        UploadPartResult uploadPartResult = new UploadPartResult();
        uploadPartResult.setETag("eTag");
        when(s3.uploadPart(any(UploadPartRequest.class))).thenReturn(uploadPartResult);

        InitiateMultipartUploadResult initiateMultipartUploadResult = new InitiateMultipartUploadResult();
        initiateMultipartUploadResult.setUploadId("uploadId");
        when(s3.initiateMultipartUpload(any(InitiateMultipartUploadRequest.class))).thenReturn(initiateMultipartUploadResult);

        CompleteMultipartUploadResult completeMultipartUploadResult = new CompleteMultipartUploadResult();
        completeMultipartUploadResult.setETag("eTag");
        completeMultipartUploadResult.setVersionId("versionId");
        when(s3.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(completeMultipartUploadResult);

        when(s3.listParts(any(ListPartsRequest.class))).thenReturn(new PartListing());

        int testFileSizeInBytes = 128;
        Path testFile = setupFile(testFileSizeInBytes);
        StreamContext streamContext = new StreamContext(getStreamSuppliers(testFile, 25, testFileSizeInBytes), testFileSizeInBytes);

        Upload upload = multipartTransferManager.upload(
            "sample_bucket",
            "sample_file_name",
            null,
            null,
            streamContext,
            null,
            WritePriority.HIGH
        );

        upload.waitForCompletion();

        verify(s3, times(streamContext.getStreamSuppliers().size())).uploadPart(any(UploadPartRequest.class));
    }

    public void tearDown() throws Exception {
        openInputStreams.forEach(openInputStream -> {
            try {
                openInputStream.close();
            } catch (IOException e) {
                logger.warn("Failed to close input stream");
            }
        });

        priorityRemoteUpload.shutdown();
        remoteUpload.shutdown();

        super.tearDown();
    }
}
