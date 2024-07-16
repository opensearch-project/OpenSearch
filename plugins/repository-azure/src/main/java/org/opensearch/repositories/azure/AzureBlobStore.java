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

package org.opensearch.repositories.azure;

import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.common.implementation.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.repositories.azure.AzureRepository.Repository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

public class AzureBlobStore implements BlobStore {

    private static final Logger logger = LogManager.getLogger(AzureBlobStore.class);

    private final AzureStorageService service;
    private final ThreadPool threadPool;

    private final String clientName;
    private final String container;
    private final LocationMode locationMode;

    private final Stats stats = new Stats();
    private final BiConsumer<HttpRequest, HttpResponse> metricsCollector;

    public AzureBlobStore(RepositoryMetadata metadata, AzureStorageService service, ThreadPool threadPool) {
        this.container = Repository.CONTAINER_SETTING.get(metadata.settings());
        this.clientName = Repository.CLIENT_NAME.get(metadata.settings());
        this.service = service;
        this.threadPool = threadPool;
        // locationMode is set per repository, not per client
        this.locationMode = Repository.LOCATION_MODE_SETTING.get(metadata.settings());
        final Map<String, AzureStorageSettings> prevSettings = this.service.refreshAndClearCache(emptyMap());
        final Map<String, AzureStorageSettings> newSettings = AzureStorageSettings.overrideLocationMode(prevSettings, this.locationMode);
        this.service.refreshAndClearCache(newSettings);

        this.metricsCollector = (request, response) -> {
            if (response.getStatusCode() >= 300) {
                return;
            }

            final HttpMethod method = request.getHttpMethod();
            if (method.equals(HttpMethod.HEAD)) {
                stats.headOperations.incrementAndGet();
                return;
            } else if (method.equals(HttpMethod.GET)) {
                final String query = request.getUrl().getQuery();
                final String queryParams = (query == null) ? "" : query;
                if (queryParams.contains("comp=list")) {
                    stats.listOperations.incrementAndGet();
                } else {
                    stats.getOperations.incrementAndGet();
                }
            } else if (method.equals(HttpMethod.PUT)) {
                final String query = request.getUrl().getQuery();
                final String queryParams = (query == null) ? "" : query;

                // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block
                // https://docs.microsoft.com/en-us/rest/api/storageservices/put-block-list
                if (queryParams.contains("comp=block") && queryParams.contains("blockid=")) {
                    stats.putBlockOperations.incrementAndGet();
                } else if (queryParams.contains("comp=blocklist")) {
                    stats.putBlockListOperations.incrementAndGet();
                } else {
                    // https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#uri-parameters
                    // The only URI parameter allowed for put-blob operation is "timeout", but if a sas token is used,
                    // it's possible that the URI parameters contain additional parameters unrelated to the upload type.
                    stats.putOperations.incrementAndGet();
                }
            }
        };
    }

    @Override
    public String toString() {
        return container;
    }

    public AzureStorageService getService() {
        return service;
    }

    /**
     * Gets the configured {@link LocationMode} for the Azure storage requests.
     */
    public LocationMode getLocationMode() {
        return locationMode;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new AzureBlobContainer(path, this, threadPool);
    }

    @Override
    public void close() throws IOException {
        service.close();
    }

    public boolean blobExists(String blob) throws URISyntaxException, BlobStorageException {
        // Container name must be lower case.
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        return SocketAccess.doPrivilegedException(() -> {
            final BlobClient azureBlob = blobContainer.getBlobClient(blob);
            final Response<Boolean> response = azureBlob.existsWithResponse(timeout(), client.v2().get());
            return response.getValue();
        });
    }

    public void deleteBlob(String blob) throws URISyntaxException, BlobStorageException {
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        // Container name must be lower case.
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        logger.trace(() -> new ParameterizedMessage("delete blob for container [{}], blob [{}]", container, blob));
        SocketAccess.doPrivilegedVoidException(() -> {
            final BlobClient azureBlob = blobContainer.getBlobClient(blob);
            logger.trace(() -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blob));
            final Response<Void> response = azureBlob.deleteWithResponse(null, null, timeout(), client.v2().get());
            logger.trace(
                () -> new ParameterizedMessage("container [{}]: blob [{}] deleted status [{}].", container, blob, response.getStatusCode())
            );
        });
    }

    public DeleteResult deleteBlobDirectory(String path, Executor executor) throws URISyntaxException, BlobStorageException, IOException {
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final Collection<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final AtomicLong outstanding = new AtomicLong(1L);
        final PlainActionFuture<Void> result = PlainActionFuture.newFuture();
        final AtomicLong blobsDeleted = new AtomicLong();
        final AtomicLong bytesDeleted = new AtomicLong();
        final ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setPrefix(path);

        SocketAccess.doPrivilegedVoidException(() -> {
            for (final BlobItem blobItem : blobContainer.listBlobs(listBlobsOptions, timeout())) {
                // Skipping prefixes as those are not deletable and should not be there
                assert (blobItem.isPrefix() == null || !blobItem.isPrefix()) : "Only blobs (not prefixes) are expected";

                outstanding.incrementAndGet();
                executor.execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        final long len = blobItem.getProperties().getContentLength();

                        final BlobClient azureBlob = blobContainer.getBlobClient(blobItem.getName());
                        logger.trace(
                            () -> new ParameterizedMessage("container [{}]: blob [{}] found. removing.", container, blobItem.getName())
                        );
                        final Response<Void> response = azureBlob.deleteWithResponse(null, null, timeout(), client.v2().get());
                        logger.trace(
                            () -> new ParameterizedMessage(
                                "container [{}]: blob [{}] deleted status [{}].",
                                container,
                                blobItem.getName(),
                                response.getStatusCode()
                            )
                        );

                        blobsDeleted.incrementAndGet();
                        if (len >= 0) {
                            bytesDeleted.addAndGet(len);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        exceptions.add(e);
                    }

                    @Override
                    public void onAfter() {
                        if (outstanding.decrementAndGet() == 0) {
                            result.onResponse(null);
                        }
                    }
                });
            }
        });
        if (outstanding.decrementAndGet() == 0) {
            result.onResponse(null);
        }
        result.actionGet();
        if (exceptions.isEmpty() == false) {
            final IOException ex = new IOException("Deleting directory [" + path + "] failed");
            exceptions.forEach(ex::addSuppressed);
            throw ex;
        }
        return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
    }

    public InputStream getInputStream(String blob, long position, @Nullable Long length) throws URISyntaxException, BlobStorageException {
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final BlobClient azureBlob = blobContainer.getBlobClient(blob);
        logger.trace(() -> new ParameterizedMessage("reading container [{}], blob [{}]", container, blob));

        return SocketAccess.doPrivilegedException(() -> {
            if (length == null) {
                return azureBlob.openInputStream(new BlobRange(position), null);
            } else {
                return azureBlob.openInputStream(new BlobRange(position, length), null);
            }
        });
    }

    public Map<String, BlobMetadata> listBlobsByPrefix(String keyPath, String prefix) throws URISyntaxException, BlobStorageException {
        final Map<String, BlobMetadata> blobsBuilder = new HashMap<String, BlobMetadata>();
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        logger.trace(() -> new ParameterizedMessage("listing container [{}], keyPath [{}], prefix [{}]", container, keyPath, prefix));

        // NOTE: this should be here: if (prefix == null) prefix = "";
        // however, this is really inefficient since deleteBlobsByPrefix enumerates everything and
        // then does a prefix match on the result; it should just call listBlobsByPrefix with the prefix!
        final ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setDetails(new BlobListDetails().setRetrieveMetadata(true))
            .setPrefix(keyPath + (prefix == null ? "" : prefix));

        SocketAccess.doPrivilegedVoidException(() -> {
            for (final BlobItem blobItem : blobContainer.listBlobsByHierarchy("/", listBlobsOptions, timeout())) {
                // Skipping over the prefixes, only look for the blobs
                if (blobItem.isPrefix() != null && blobItem.isPrefix()) {
                    continue;
                }

                final String name = getBlobName(blobItem.getName(), container, keyPath);
                logger.trace(() -> new ParameterizedMessage("blob name [{}]", name));

                final BlobItemProperties properties = blobItem.getProperties();
                logger.trace(() -> new ParameterizedMessage("blob name [{}], size [{}]", name, properties.getContentLength()));
                blobsBuilder.put(name, new PlainBlobMetadata(name, properties.getContentLength()));
            }
        });

        return MapBuilder.newMapBuilder(blobsBuilder).immutableMap();
    }

    public Map<String, BlobContainer> children(BlobPath path) throws URISyntaxException, BlobStorageException {
        final Set<String> blobsBuilder = new HashSet<String>();
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final String keyPath = path.buildAsString();

        final ListBlobsOptions listBlobsOptions = new ListBlobsOptions().setDetails(new BlobListDetails().setRetrieveMetadata(true))
            .setPrefix(keyPath);

        SocketAccess.doPrivilegedVoidException(() -> {
            for (final BlobItem blobItem : blobContainer.listBlobsByHierarchy("/", listBlobsOptions, timeout())) {
                // Skipping over the blobs, only look for prefixes
                if (blobItem.isPrefix() != null && blobItem.isPrefix()) {
                    // Expecting name in the form /container/keyPath.* and we want to strip off the /container/
                    // this requires 1 + container.length() + 1, with each 1 corresponding to one of the /.
                    // Lastly, we add the length of keyPath to the offset to strip this container's path.
                    final String name = getBlobName(blobItem.getName(), container, keyPath).replaceAll("/$", "");
                    logger.trace(() -> new ParameterizedMessage("blob name [{}]", name));
                    blobsBuilder.add(name);
                }
            }
        });

        return Collections.unmodifiableMap(
            blobsBuilder.stream()
                .collect(Collectors.toMap(Function.identity(), name -> new AzureBlobContainer(path.add(name), this, threadPool)))
        );
    }

    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws URISyntaxException,
        BlobStorageException, IOException {
        assert inputStream.markSupported()
            : "Should not be used with non-mark supporting streams as their retry handling in the SDK is broken";
        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {})", blobName, blobSize));
        final Tuple<BlobServiceClient, Supplier<Context>> client = client();
        final BlobContainerClient blobContainer = client.v1().getBlobContainerClient(container);
        final BlobClient blob = blobContainer.getBlobClient(blobName);
        try {
            final BlobRequestConditions blobRequestConditions = new BlobRequestConditions();
            if (failIfAlreadyExists) {
                blobRequestConditions.setIfNoneMatch(Constants.HeaderConstants.ETAG_WILDCARD);
            }

            SocketAccess.doPrivilegedVoidException(() -> {
                final Response<?> response = blob.uploadWithResponse(
                    new BlobParallelUploadOptions(inputStream, blobSize).setRequestConditions(blobRequestConditions)
                        .setParallelTransferOptions(service.getBlobRequestOptionsForWriteBlob()),
                    timeout(),
                    client.v2().get()
                );
                logger.trace(
                    () -> new ParameterizedMessage("upload({}, stream, {}) - status [{}]", blobName, blobSize, response.getStatusCode())
                );
            });
        } catch (final BlobStorageException se) {
            if (failIfAlreadyExists
                && se.getStatusCode() == HttpURLConnection.HTTP_CONFLICT
                && BlobErrorCode.BLOB_ALREADY_EXISTS.equals(se.getErrorCode())) {
                throw new FileAlreadyExistsException(blobName, null, se.getMessage());
            }
            throw se;
        } catch (final RuntimeException ex) {
            // Since most of the logic is happening inside the reactive pipeline, the checked exceptions
            // are swallowed and wrapped into runtime one (see please Exceptions.ReactiveException).
            if (ex.getCause() != null) {
                Throwables.rethrow(ex.getCause());
            } else {
                throw ex;
            }
        }

        logger.trace(() -> new ParameterizedMessage("writeBlob({}, stream, {}) - done", blobName, blobSize));
    }

    private Tuple<BlobServiceClient, Supplier<Context>> client() {
        return service.client(clientName, metricsCollector);
    }

    private Duration timeout() {
        return service.getBlobRequestTimeout(clientName);
    }

    @Override
    public Map<String, Long> stats() {
        return stats.toMap();
    }

    /**
     * Extracts the name of the blob from path or prefixed blob name
     * @param pathOrName prefixed blob name or blob path
     * @param container container
     * @param keyPath key path
     * @return blob name
     */
    private String getBlobName(final String pathOrName, final String container, final String keyPath) {
        String name = pathOrName;

        if (name.matches("." + container + ".")) {
            name = name.substring(1 + container.length() + 1);
        }

        if (name.startsWith(keyPath)) {
            name = name.substring(keyPath.length());
        }

        return name;
    }

    private static class Stats {

        private final AtomicLong getOperations = new AtomicLong();

        private final AtomicLong listOperations = new AtomicLong();

        private final AtomicLong headOperations = new AtomicLong();

        private final AtomicLong putOperations = new AtomicLong();

        private final AtomicLong putBlockOperations = new AtomicLong();

        private final AtomicLong putBlockListOperations = new AtomicLong();

        private Map<String, Long> toMap() {
            return Map.of(
                "GetBlob",
                getOperations.get(),
                "ListBlobs",
                listOperations.get(),
                "GetBlobProperties",
                headOperations.get(),
                "PutBlob",
                putOperations.get(),
                "PutBlock",
                putBlockOperations.get(),
                "PutBlockList",
                putBlockListOperations.get()
            );
        }
    }
}
