/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ExecutorService;

/**
 * Abstract class for a blob type storage
 *
 * @param <T> The entity which can be uploaded to / downloaded from blob store
 * @param <U> The concrete class implementing {@link RemoteWriteableEntity} which is used as a wrapper for T entity.
 */
public class RemoteWriteableEntityBlobStore<T, U extends RemoteWriteableBlobEntity<T>> implements RemoteWritableEntityStore<T, U> {

    private static final Logger logger = LogManager.getLogger(RemoteWriteableEntityBlobStore.class);
    private final BlobStoreTransferService transferService;
    private final BlobStoreRepository blobStoreRepository;
    private final String clusterName;
    private final ExecutorService executorService;
    private final String pathToken;
    /**
     * To be used for identifying and logging read tasks/entities which take considerably more time in getting completed.
     * Threshold corresponds to the total time spent in reading the blob along with deserializing the i/p stream.
     */
    public static final Setting<TimeValue> REMOTE_STORE_SLOW_READ_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "cluster.remote_store.slow_read_logging_threshold",
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private volatile TimeValue slowReadLoggingThreshold;

    public RemoteWriteableEntityBlobStore(
        final BlobStoreTransferService blobStoreTransferService,
        final BlobStoreRepository blobStoreRepository,
        final String clusterName,
        final ThreadPool threadPool,
        final String executor,
        final String pathToken,
        final ClusterSettings clusterSettings
    ) {
        this.transferService = blobStoreTransferService;
        this.blobStoreRepository = blobStoreRepository;
        this.clusterName = clusterName;
        this.executorService = threadPool.executor(executor);
        this.pathToken = pathToken;
        this.slowReadLoggingThreshold = clusterSettings.get(REMOTE_STORE_SLOW_READ_LOGGING_THRESHOLD_SETTING);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_STORE_SLOW_READ_LOGGING_THRESHOLD_SETTING, this::setSlowReadLoggingThreshold);
    }

    private void setSlowReadLoggingThreshold(TimeValue slowReadLoggingThreshold) {
        this.slowReadLoggingThreshold = slowReadLoggingThreshold;
    }

    @Override
    public void writeAsync(final U entity, final ActionListener<Void> listener) {
        try {
            try (InputStream inputStream = entity.serialize()) {
                BlobPath blobPath = getBlobPathForUpload(entity);
                entity.setFullBlobName(blobPath);
                transferService.uploadBlob(
                    inputStream,
                    getBlobPathForUpload(entity),
                    entity.getBlobFileName(),
                    WritePriority.URGENT,
                    listener
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public T read(final U entity) throws IOException {
        assert entity.getFullBlobName() != null;
        try (InputStream inputStream = transferService.downloadBlob(getBlobPathForDownload(entity), entity.getBlobFileName())) {
            return entity.deserialize(inputStream);
        }
    }

    public ReadBlobWithMetrics<T> readWithMetrics(final U entity) throws IOException {
        assert entity.getFullBlobName() != null;
        final long readStartTimeNS = System.nanoTime();
        try (InputStream inputStream = transferService.downloadBlob(getBlobPathForDownload(entity), entity.getBlobFileName())) {
            final long deserializeStartTimeNS = System.nanoTime();
            T deserializedBlobEntity = entity.deserialize(inputStream);
            long totalReadTimeMS = Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - readStartTimeNS));
            long serdeTimeMS = Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - deserializeStartTimeNS));
            warnAboutSlowReadIfNeeded(entity, serdeTimeMS, totalReadTimeMS);
            return new ReadBlobWithMetrics<>(deserializedBlobEntity, serdeTimeMS, totalReadTimeMS - serdeTimeMS);
        }
    }

    @Override
    public void readAsync(final U entity, final ActionListener<T> listener) {
        executorService.execute(() -> {
            try {
                listener.onResponse(read(entity));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public void readAsyncWithMetrics(final U entity, final ActionListener<ReadBlobWithMetrics<T>> listener) {
        final long queueStartTimeNS = System.nanoTime();
        executorService.execute(() -> {
            try {
                ReadBlobWithMetrics<T> result = readWithMetrics(entity);
                listener.onResponse(result);
                final long executionTimeMS = Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - queueStartTimeNS));
                if (executionTimeMS > slowReadLoggingThreshold.getMillis()) {
                    logger.warn(
                        "entity [{}] for [{}] took total [{}] ms to be executed with time spent in reading {} ms and de-serializing {} ms",
                        entity.getClass().getSimpleName(),
                        entity.getBlobFileName(),
                        executionTimeMS,
                        result.readMS(),
                        result.serDeMS()
                    );
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public String getClusterName() {
        return clusterName;
    }

    public BlobPath getBlobPathPrefix(String clusterUUID) {
        return blobStoreRepository.basePath().add(encodeString(getClusterName())).add(pathToken).add(clusterUUID);
    }

    public BlobPath getBlobPathForUpload(final RemoteWriteableBlobEntity<T> obj) {
        BlobPath blobPath = getBlobPathPrefix(obj.clusterUUID());
        for (String token : obj.getBlobPathParameters().getPathTokens()) {
            blobPath = blobPath.add(token);
        }
        return obj.getPrefixedPath(blobPath);
    }

    public BlobPath getBlobPathForDownload(final RemoteWriteableBlobEntity<T> obj) {
        String[] pathTokens = obj.getBlobPathTokens();
        BlobPath blobPath = new BlobPath();
        if (pathTokens == null || pathTokens.length < 1) {
            return blobPath;
        }
        // Iterate till second last path token to get the blob folder
        for (int i = 0; i < pathTokens.length - 1; i++) {
            blobPath = blobPath.add(pathTokens[i]);
        }
        return blobPath;
    }

    private static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    private void warnAboutSlowReadIfNeeded(final U entity, final long serdeTimeMS, final long totalReadTimeMS) {
        if (totalReadTimeMS > slowReadLoggingThreshold.getMillis()) {
            logger.warn(
                "entity [{}] for [{}] took [{}] for serde out of total read time [{}] which is above the total warn threshold of [{}]",
                entity.getClass().getSimpleName(),
                entity.getBlobFileName(),
                serdeTimeMS,
                totalReadTimeMS,
                slowReadLoggingThreshold
            );
        }
    }
}
