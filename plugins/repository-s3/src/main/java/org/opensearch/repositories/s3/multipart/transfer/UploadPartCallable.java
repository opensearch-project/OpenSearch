/*
 * Copyright 2010-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.multipart.transfer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;

import com.amazonaws.SdkClientException;
import com.amazonaws.internal.SdkThreadLocalsRegistry;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.BinaryUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.repositories.s3.SocketAccess;

public class UploadPartCallable implements Callable<PartETag> {
    private static final int MAX_SKIPS = 100;
    private static final ThreadLocal<MessageDigest> MD5_DIGEST;
    private static final Logger log = LogManager.getLogger(UploadPartCallable.class);
    static {
        MD5_DIGEST = SdkThreadLocalsRegistry.register(new ThreadLocal<MessageDigest>() {
            @Override
            protected MessageDigest initialValue() {
                try {
                    return MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new SdkClientException("Unable to get a digest instance for MD5!", e);
                }
            }
        });
    }
    private final AmazonS3 s3;
    private final UploadPartRequest request;
    private final boolean calculateMd5;
    private final String fileName;

    public UploadPartCallable(AmazonS3 s3, UploadPartRequest request, boolean calculateMd5, String fileName) {
        this.s3 = s3;
        this.request = request;
        this.calculateMd5 = calculateMd5;
        this.fileName = fileName;
    }

    public PartETag call() throws Exception {
        if (calculateMd5) {
            request.withMD5Digest(computedMd5());
        }

        try {
            PartETag partETag = SocketAccess.doPrivileged(() -> s3.uploadPart(request).getPartETag());
            return partETag;
        } catch (Exception e) {
            log.error("Failed UploadPartCallable for file {}, part number {}, part size {}",
                fileName, request.getPartNumber(), request.getPartSize());
            throw e;
        }
    }


    private String computedMd5() {
        FileInputStream fileStream = null;
        try {
            fileStream = new FileInputStream(request.getFile());
            skipBytes(fileStream, request.getFileOffset());
            return BinaryUtils.toBase64(computeMd5Bytes(fileStream, request.getPartSize()));
        } catch (IOException e) {
            throw new SdkClientException(e);
        } finally {
            if (fileStream != null) {
                try {
                    fileStream.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static byte[] computeMd5Bytes(InputStream is, long remaining) throws IOException {
        byte readBuff[] = new byte[4096];
        MessageDigest digest = MD5_DIGEST.get();
        digest.reset();
        int read;
        while (remaining > 0 && (read = is.read(readBuff)) != -1) {
            int updateLen = (int) Math.min(remaining, read);
            digest.update(readBuff, 0, updateLen);
            remaining -= updateLen;
        }
        return digest.digest();
    }

    private void skipBytes(FileInputStream fs, long n) throws IOException {
        long skippedSoFar = 0;
        for (int skips = 0; skips < MAX_SKIPS && skippedSoFar < n; ++skips) {
            skippedSoFar += fs.skip(n - skippedSoFar);
        }
        if (skippedSoFar != n) {
            throw new SdkClientException(String.format("Unable to skip to offset %d in file %s after %d attempts",
                n, request.getFile().getAbsolutePath(), MAX_SKIPS));
        }
    }
}
