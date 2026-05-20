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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.blobstore.support;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.InputStreamWithMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Filter blob container.
 *
 * @opensearch.internal
 */
public abstract class FilterBlobContainer implements BlobContainer {

    private final BlobContainer delegate;

    public FilterBlobContainer(BlobContainer delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    protected abstract BlobContainer wrapChild(BlobContainer child);

    @Override
    public BlobPath path() {
        return delegate.path();
    }

    @Override
    public boolean blobExists(String blobName) throws IOException {
        return delegate.blobExists(blobName);
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return delegate.readBlob(blobName);
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        return delegate.readBlob(blobName, position, length);
    }

    @Override
    public long readBlobPreferredLength() {
        return delegate.readBlobPreferredLength();
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        delegate.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
        return delegate.readBlobWithMetadata(blobName);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata
    ) throws IOException {
        delegate.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata);
    }

    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {
        delegate.writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata, cryptoMetadata);
    }

    @Override
    public void writeBlobAtomicWithMetadata(
        String blobName,
        InputStream inputStream,
        @Nullable Map<String, String> metadata,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        delegate.writeBlobAtomicWithMetadata(blobName, inputStream, metadata, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        return delegate.delete();
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        delegate.deleteBlobsIgnoringIfNotExists(blobNames);
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return delegate.listBlobs();
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        return delegate.children().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> wrapChild(e.getValue())));
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        return delegate.listBlobsByPrefix(blobNamePrefix);
    }
}
