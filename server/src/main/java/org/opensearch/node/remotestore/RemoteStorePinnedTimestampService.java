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
        long startTime = System.nanoTime();
        try {
            logger.debug("Pinning timestamp = {} against entity = {}", timestamp, pinningEntity);
            blobContainer.writeBlob(getBlobName(timestamp, pinningEntity), new ByteArrayInputStream(new byte[0]), 0, true);
            long elapsedTime = System.nanoTime() - startTime;
            if (elapsedTime > RemoteStoreSettings.getPinnedTimestampsLookbackInterval().nanos()) {
                String errorMessage = String.format(
                    "Timestamp pinning took %s nanoseconds which is more than limit of %s nanoseconds, failing the operation",
                    elapsedTime,
                    RemoteStoreSettings.getPinnedTimestampsLookbackInterval().nanos()
                );
                unpinTimestamp(timestamp, pinningEntity, ActionListener.wrap(() -> listener.onFailure(new RuntimeException(errorMessage))));
            } else {
                listener.onResponse(null);
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * Clones a timestamp by creating a new pinning entity for an existing timestamp.
     *
     * This method attempts to create a new pinning entity for a given timestamp that is already
     * associated with an existing pinning entity. If the timestamp exists for the existing entity,
     * a new blob is created for the new pinning entity. If the timestamp doesn't exist for the
     * existing entity, the operation fails with an IllegalArgumentException.
     *
     * @param timestamp The timestamp to be cloned.
     * @param existingPinningEntity The name of the existing entity that has pinned the timestamp.
     * @param newPinningEntity The name of the new entity to pin the timestamp to.
     * @param listener An ActionListener that will be notified of the operation's success or failure.
     *                 On success, onResponse will be called with null. On failure, onFailure will
     *                 be called with the appropriate exception.
     */
    public void cloneTimestamp(long timestamp, String existingPinningEntity, String newPinningEntity, ActionListener<Void> listener) {
        try {
            logger.debug(
                "cloning timestamp = {} with existing pinningEntity = {} with new pinningEntity = {}",
                timestamp,
                existingPinningEntity,
                newPinningEntity
            );
            String blobName = getBlobName(timestamp, existingPinningEntity);
            if (blobContainer.blobExists(blobName)) {
                logger.debug("Pinning timestamp = {} against entity = {}", timestamp, newPinningEntity);
                blobContainer.writeBlob(getBlobName(timestamp, newPinningEntity), new ByteArrayInputStream(new byte[0]), 0, true);
                listener.onResponse(null);
            } else {
                String errorMessage = String.format("Timestamp: %s is not pinned by existing entity: %s", timestamp, existingPinningEntity);
                listener.onFailure(new IllegalArgumentException(errorMessage));
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
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
                listener.onResponse(null);
            } else {
                String errorMessage = String.format("Timestamp: %s is not pinned by entity: %s", timestamp, pinningEntity);
                listener.onFailure(new IllegalArgumentException(errorMessage));
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }
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
