/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Service for managing pinned timestamps in a remote store.
 * This service handles pinning and unpinning of timestamps, as well as periodic updates of the pinned timestamps set.
 *
 * @opensearch.internal
 */
public class RemoteStorePinnedTimestampService implements Closeable {
    private static final Logger logger = LogManager.getLogger(RemoteStorePinnedTimestampService.class);
    private static Tuple<Long, Set<Long>> pinnedTimestampsSet = new Tuple<>(-1L, Set.of());
    public static final String PINNED_TIMESTAMPS_PATH_TOKEN = "pinned_timestamps";
    public static final String PINNED_TIMESTAMPS_FILENAME_SEPARATOR = "__";

    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private BlobContainer blobContainer;
    private AsyncUpdatePinnedTimestampTask asyncUpdatePinnedTimestampTask;

    public RemoteStorePinnedTimestampService(
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * Starts the RemoteStorePinnedTimestampService.
     * This method validates the remote store configuration, initializes components,
     * and starts the asynchronous update task.
     */
    public void start() {
        validateRemoteStoreConfiguration();
        startAsyncUpdateTask(RemoteStoreSettings.getPinnedTimestampsSchedulerInterval());
    }

    private void validateRemoteStoreConfiguration() {
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Segment Store repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        blobContainer = blobStoreRepository.blobStore().blobContainer(blobStoreRepository.basePath().add(PINNED_TIMESTAMPS_PATH_TOKEN));
    }

    private void startAsyncUpdateTask(TimeValue pinnedTimestampsSchedulerInterval) {
        asyncUpdatePinnedTimestampTask = new AsyncUpdatePinnedTimestampTask(logger, threadPool, pinnedTimestampsSchedulerInterval, true);
    }

    /**
     * Pins a timestamp in the remote store.
     *
     * @param timestamp The timestamp to be pinned
     * @param pinningEntity The entity responsible for pinning the timestamp
     * @param listener A listener to be notified when the pinning operation completes
     * @throws IllegalArgumentException If the timestamp is less than the current time minus one second
     */
    public void pinTimestamp(long timestamp, String pinningEntity, ActionListener<Void> listener) {
        // If a caller uses current system time to pin the timestamp, following check will almost always fail.
        // So, we allow pinning timestamp in the past upto some buffer
        long lookbackIntervalInMills = RemoteStoreSettings.getPinnedTimestampsLookbackInterval().millis();
        if (timestamp < (System.currentTimeMillis() - lookbackIntervalInMills)) {
            throw new IllegalArgumentException(
                "Timestamp to be pinned is less than current timestamp - value of cluster.remote_store.pinned_timestamps.lookback_interval"
            );
        }
        try {
            logger.debug("Pinning timestamp = {} against entity = {}", timestamp, pinningEntity);
            blobContainer.writeBlob(getBlobName(timestamp, pinningEntity), new ByteArrayInputStream(new byte[0]), 0, true);
        } catch (IOException e) {
            listener.onFailure(e);
        }
        listener.onResponse(null);
    }

    private String getBlobName(long timestamp, String pinningEntity) {
        return String.join(PINNED_TIMESTAMPS_FILENAME_SEPARATOR, pinningEntity, String.valueOf(timestamp));
    }

    private long getTimestampFromBlobName(String blobName) {
        String[] blobNameTokens = blobName.split(PINNED_TIMESTAMPS_FILENAME_SEPARATOR);
        if (blobNameTokens.length < 2) {
            logger.error("Pinned timestamps blob name contains invalid format: {}", blobName);
        }
        try {
            return Long.parseLong(blobNameTokens[blobNameTokens.length - 1]);
        } catch (NumberFormatException e) {
            logger.error(() -> new ParameterizedMessage("Pinned timestamps blob name contains invalid format: {}", blobName), e);
        }
        return -1;
    }

    /**
     * Unpins a timestamp from the remote store.
     *
     * @param timestamp The timestamp to be unpinned
     * @param pinningEntity The entity responsible for unpinning the timestamp
     * @param listener A listener to be notified when the unpinning operation completes
     */
    public void unpinTimestamp(long timestamp, String pinningEntity, ActionListener<Void> listener) {
        try {
            logger.debug("Unpinning timestamp = {} against entity = {}", timestamp, pinningEntity);
            String blobName = getBlobName(timestamp, pinningEntity);
            if (blobContainer.blobExists(blobName)) {
                blobContainer.deleteBlobsIgnoringIfNotExists(List.of(blobName));
            } else {
                logger.warn("Timestamp: {} is not pinned by entity: {}", timestamp, pinningEntity);
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
        listener.onResponse(null);
    }

    @Override
    public void close() throws IOException {
        asyncUpdatePinnedTimestampTask.close();
    }

    // Used in integ tests
    public void rescheduleAsyncUpdatePinnedTimestampTask(TimeValue pinnedTimestampsSchedulerInterval) {
        if (pinnedTimestampsSchedulerInterval != null) {
            pinnedTimestampsSet = new Tuple<>(-1L, Set.of());
            asyncUpdatePinnedTimestampTask.close();
            startAsyncUpdateTask(pinnedTimestampsSchedulerInterval);
        }
    }

    public static Tuple<Long, Set<Long>> getPinnedTimestamps() {
        return pinnedTimestampsSet;
    }

    /**
     * Inner class for asynchronously updating the pinned timestamp set.
     */
    private final class AsyncUpdatePinnedTimestampTask extends AbstractAsyncTask {
        private AsyncUpdatePinnedTimestampTask(Logger logger, ThreadPool threadPool, TimeValue interval, boolean autoReschedule) {
            super(logger, threadPool, interval, autoReschedule);
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            long triggerTimestamp = System.currentTimeMillis();
            try {
                Map<String, BlobMetadata> pinnedTimestampList = blobContainer.listBlobs();
                if (pinnedTimestampList.isEmpty()) {
                    pinnedTimestampsSet = new Tuple<>(triggerTimestamp, Set.of());
                    return;
                }
                Set<Long> pinnedTimestamps = pinnedTimestampList.keySet()
                    .stream()
                    .map(RemoteStorePinnedTimestampService.this::getTimestampFromBlobName)
                    .filter(timestamp -> timestamp != -1)
                    .collect(Collectors.toSet());
                logger.debug("Fetched pinned timestamps from remote store: {} - {}", triggerTimestamp, pinnedTimestamps);
                pinnedTimestampsSet = new Tuple<>(triggerTimestamp, pinnedTimestamps);
            } catch (Throwable t) {
                logger.error("Exception while fetching pinned timestamp details", t);
            }
        }
    }
}
