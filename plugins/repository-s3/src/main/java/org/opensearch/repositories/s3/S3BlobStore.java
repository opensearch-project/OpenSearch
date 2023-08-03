/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.core.common.unit.ByteSizeValue;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.StorageClass;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferManager;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

class S3BlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(S3BlobStore.class);

    private final S3Service service;

    private final S3AsyncService s3AsyncService;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final boolean serverSideEncryption;

    private final ObjectCannedACL cannedACL;

    private final StorageClass storageClass;

    private final RepositoryMetadata repositoryMetadata;

    private final StatsMetricPublisher statsMetricPublisher = new StatsMetricPublisher();

    private final AsyncTransferManager asyncTransferManager;
    private final AsyncExecutorContainer priorityExecutorBuilder;
    private final AsyncExecutorContainer normalExecutorBuilder;
    private final boolean multipartUploadEnabled;

    S3BlobStore(
        S3Service service,
        S3AsyncService s3AsyncService,
        boolean multipartUploadEnabled,
        String bucket,
        boolean serverSideEncryption,
        ByteSizeValue bufferSize,
        String cannedACL,
        String storageClass,
        RepositoryMetadata repositoryMetadata,
        AsyncTransferManager asyncTransferManager,
        AsyncExecutorContainer priorityExecutorBuilder,
        AsyncExecutorContainer normalExecutorBuilder
    ) {
        this.service = service;
        this.s3AsyncService = s3AsyncService;
        this.multipartUploadEnabled = multipartUploadEnabled;
        this.bucket = bucket;
        this.serverSideEncryption = serverSideEncryption;
        this.bufferSize = bufferSize;
        this.cannedACL = initCannedACL(cannedACL);
        this.storageClass = initStorageClass(storageClass);
        this.repositoryMetadata = repositoryMetadata;
        this.asyncTransferManager = asyncTransferManager;
        this.normalExecutorBuilder = normalExecutorBuilder;
        this.priorityExecutorBuilder = priorityExecutorBuilder;
    }

    public boolean isMultipartUploadEnabled() {
        return multipartUploadEnabled;
    }

    @Override
    public String toString() {
        return bucket;
    }

    public AmazonS3Reference clientReference() {
        return service.client(repositoryMetadata);
    }

    public AmazonAsyncS3Reference asyncClientReference() {
        return s3AsyncService.client(repositoryMetadata, priorityExecutorBuilder, normalExecutorBuilder);
    }

    int getMaxRetries() {
        return service.settings(repositoryMetadata).maxRetries;
    }

    public String bucket() {
        return bucket;
    }

    public boolean serverSideEncryption() {
        return serverSideEncryption;
    }

    public long bufferSizeInBytes() {
        return bufferSize.getBytes();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new S3BlobContainer(path, this);
    }

    @Override
    public void close() throws IOException {
        if (service != null) {
            this.service.close();
        }
        if (s3AsyncService != null) {
            this.s3AsyncService.close();
        }
    }

    @Override
    public Map<String, Long> stats() {
        return statsMetricPublisher.getStats().toMap();
    }

    public ObjectCannedACL getCannedACL() {
        return cannedACL;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public StatsMetricPublisher getStatsMetricPublisher() {
        return statsMetricPublisher;
    }

    public static StorageClass initStorageClass(String storageClassStringValue) {
        if ((storageClassStringValue == null) || storageClassStringValue.equals("")) {
            return StorageClass.STANDARD;
        }

        final StorageClass storageClass = StorageClass.fromValue(storageClassStringValue.toUpperCase(Locale.ENGLISH));
        if (storageClass.equals(StorageClass.GLACIER)) {
            throw new BlobStoreException("Glacier storage class is not supported");
        }

        if (storageClass == StorageClass.UNKNOWN_TO_SDK_VERSION) {
            throw new BlobStoreException("`" + storageClassStringValue + "` is not a valid S3 Storage Class.");
        }

        return storageClass;
    }

    /**
     * Constructs canned acl from string
     */
    public static ObjectCannedACL initCannedACL(String cannedACL) {
        if ((cannedACL == null) || cannedACL.equals("")) {
            return ObjectCannedACL.PRIVATE;
        }

        for (final ObjectCannedACL cur : ObjectCannedACL.values()) {
            if (cur.toString().equalsIgnoreCase(cannedACL)) {
                return cur;
            }
        }

        throw new BlobStoreException("cannedACL is not valid: [" + cannedACL + "]");
    }

    public AsyncTransferManager getAsyncTransferManager() {
        return asyncTransferManager;
    }
}
