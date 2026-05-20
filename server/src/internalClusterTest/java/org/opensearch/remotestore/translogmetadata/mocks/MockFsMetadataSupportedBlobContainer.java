/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.translogmetadata.mocks;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.remotestore.multipart.mocks.MockFsAsyncBlobContainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class MockFsMetadataSupportedBlobContainer extends MockFsAsyncBlobContainer {

    private static String CHECKPOINT_FILE_DATA_KEY = "ckp-data";

    public MockFsMetadataSupportedBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, boolean triggerDataIntegrityFailure) {
        super(blobStore, blobPath, path, triggerDataIntegrityFailure);
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        // If the upload writeContext have a non-null metadata, we store the metadata content as translog.ckp file.
        if (writeContext.getMetadata() != null) {
            String base64String = writeContext.getMetadata().get(CHECKPOINT_FILE_DATA_KEY);
            byte[] decodedBytes = Base64.getDecoder().decode(base64String);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(decodedBytes);
            int length = decodedBytes.length;
            String ckpFileName = getCheckpointFileName(writeContext.getFileName());
            writeBlob(ckpFileName, inputStream, length, true);
        }
        super.asyncBlobUpload(writeContext, completionListener);
    }

    // This is utility to get the translog.ckp file name for a given translog.tlog file.
    private String getCheckpointFileName(String translogFileName) {
        if (!translogFileName.endsWith(".tlog")) {
            throw new IllegalArgumentException("Invalid translog file name format: " + translogFileName);
        }

        int dotIndex = translogFileName.lastIndexOf('.');
        String baseName = translogFileName.substring(0, dotIndex);
        return baseName + ".ckp";
    }

    public static String convertToBase64(InputStream inputStream) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[128];
            int bytesRead;
            int totalBytesRead = 0;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
                if (totalBytesRead > 1024) {
                    // We enforce a limit of 1KB on the size of the checkpoint file.
                    throw new AssertionError("Input stream exceeds 1KB limit");
                }
            }

            byte[] bytes = byteArrayOutputStream.toByteArray();
            return Base64.getEncoder().encodeToString(bytes);
        }
    }

    // during readBlobWithMetadata call we separately download translog.ckp file and return it as metadata.
    @Override
    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
        String ckpFileName = getCheckpointFileName(blobName);
        InputStream inputStream = readBlob(blobName);
        try (InputStream ckpInputStream = readBlob(ckpFileName)) {
            String ckpString = convertToBase64(ckpInputStream);
            Map<String, String> metadata = new HashMap<>();
            metadata.put(CHECKPOINT_FILE_DATA_KEY, ckpString);
            return new InputStreamWithMetadata(inputStream, metadata);
        }
    }
}
