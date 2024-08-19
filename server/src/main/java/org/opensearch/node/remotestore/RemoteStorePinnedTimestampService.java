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
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.model.RemotePinnedTimestamps;
import org.opensearch.gateway.remote.model.RemotePinnedTimestamps.PinnedTimestamps;
import org.opensearch.gateway.remote.model.RemoteStorePinnedTimestampsBlobStore;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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
    public static final int PINNED_TIMESTAMP_FILES_TO_KEEP = 5;

    private final Supplier<RepositoriesService> repositoriesService;
    private final Settings settings;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final RemoteStoreSettings remoteStoreSettings;
    private BlobStoreRepository blobStoreRepository;
    private BlobStoreTransferService blobStoreTransferService;
    private RemoteStorePinnedTimestampsBlobStore pinnedTimestampsBlobStore;
    private AsyncUpdatePinnedTimestampTask asyncUpdatePinnedTimestampTask;
    private final Semaphore updateTimetampPinningSemaphore = new Semaphore(1);

    public RemoteStorePinnedTimestampService(
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        RemoteStoreSettings remoteStoreSettings
    ) {
        this.repositoriesService = repositoriesService;
        this.settings = settings;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.remoteStoreSettings = remoteStoreSettings;
    }

    /**
     * Starts the RemoteStorePinnedTimestampService.
     * This method validates the remote store configuration, initializes components,
     * and starts the asynchronous update task.
     */
    public void start() {
        validateRemoteStoreConfiguration();
        initializeComponents();
        startAsyncUpdateTask(remoteStoreSettings.getPinnedTimestampsSchedulerInterval());
    }

    private void validateRemoteStoreConfiguration() {
        final String remoteStoreRepo = settings.get(
            Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY
        );
        assert remoteStoreRepo != null : "Remote Segment Store repository is not configured";
        final Repository repository = repositoriesService.get().repository(remoteStoreRepo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        blobStoreRepository = (BlobStoreRepository) repository;
    }

    private void initializeComponents() {
        String clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings).value();
        blobStoreTransferService = new BlobStoreTransferService(blobStoreRepository.blobStore(), this.threadPool);
        pinnedTimestampsBlobStore = new RemoteStorePinnedTimestampsBlobStore(
            blobStoreTransferService,
            blobStoreRepository,
            clusterName,
            this.threadPool,
            ThreadPool.Names.REMOTE_STATE_READ
        );
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
        long lookbackIntervalInMills = remoteStoreSettings.getPinnedTimestampsLookbackInterval().millis();
        if (timestamp < (System.currentTimeMillis() - lookbackIntervalInMills)) {
            throw new IllegalArgumentException(
                "Timestamp to be pinned is less than current timestamp - value of cluster.remote_store.pinned_timestamps.lookback_interval"
            );
        }
        updatePinning(pinnedTimestamps -> pinnedTimestamps.pin(timestamp, pinningEntity), listener);
    }

    /**
     * Unpins a timestamp from the remote store.
     *
     * @param timestamp The timestamp to be unpinned
     * @param pinningEntity The entity responsible for unpinning the timestamp
     * @param listener A listener to be notified when the unpinning operation completes
     */
    public void unpinTimestamp(long timestamp, String pinningEntity, ActionListener<Void> listener) {
        updatePinning(pinnedTimestamps -> pinnedTimestamps.unpin(timestamp, pinningEntity), listener);
    }

    private void updatePinning(Consumer<PinnedTimestamps> updateConsumer, ActionListener<Void> listener) {
        RemotePinnedTimestamps remotePinnedTimestamps = new RemotePinnedTimestamps(
            clusterService.state().metadata().clusterUUID(),
            blobStoreRepository.getCompressor()
        );
        BlobPath path = pinnedTimestampsBlobStore.getBlobPathForUpload(remotePinnedTimestamps);
        try {
            if (updateTimetampPinningSemaphore.tryAcquire(10, TimeUnit.MINUTES)) {
                ActionListener<Void> semaphoreAwareListener = ActionListener.runBefore(listener, updateTimetampPinningSemaphore::release);
                ActionListener<List<BlobMetadata>> listCallResponseListener = getListenerForListCallResponse(
                    remotePinnedTimestamps,
                    updateConsumer,
                    semaphoreAwareListener
                );
                blobStoreTransferService.listAllInSortedOrder(
                    path,
                    remotePinnedTimestamps.getType(),
                    Integer.MAX_VALUE,
                    listCallResponseListener
                );
            } else {
                throw new TimeoutException("Timed out while waiting to acquire lock in updatePinning");
            }
        } catch (InterruptedException | TimeoutException e) {
            listener.onFailure(e);
        }
    }

    private ActionListener<List<BlobMetadata>> getListenerForListCallResponse(
        RemotePinnedTimestamps remotePinnedTimestamps,
        Consumer<PinnedTimestamps> updateConsumer,
        ActionListener<Void> listener
    ) {
        return ActionListener.wrap(blobMetadata -> {
            PinnedTimestamps pinnedTimestamps = new PinnedTimestamps(new HashMap<>());
            if (blobMetadata.isEmpty() == false) {
                pinnedTimestamps = readExistingPinnedTimestamps(blobMetadata.get(0).name(), remotePinnedTimestamps);
            }
            updateConsumer.accept(pinnedTimestamps);
            remotePinnedTimestamps.setPinnedTimestamps(pinnedTimestamps);
            ActionListener<Void> writeCallResponseListener = getListenerForWriteCallResponse(
                remotePinnedTimestamps,
                blobMetadata,
                listener
            );
            pinnedTimestampsBlobStore.writeAsync(remotePinnedTimestamps, writeCallResponseListener);
        }, listener::onFailure);
    }

    private ActionListener<Void> getListenerForWriteCallResponse(
        RemotePinnedTimestamps remotePinnedTimestamps,
        List<BlobMetadata> blobMetadata,
        ActionListener<Void> listener
    ) {
        return ActionListener.wrap(unused -> {
            // Delete older pinnedTimestamp files
            if (blobMetadata.size() > PINNED_TIMESTAMP_FILES_TO_KEEP) {
                List<String> oldFilesToBeDeleted = blobMetadata.subList(PINNED_TIMESTAMP_FILES_TO_KEEP, blobMetadata.size())
                    .stream()
                    .map(BlobMetadata::name)
                    .collect(Collectors.toList());
                try {
                    blobStoreTransferService.deleteBlobs(
                        pinnedTimestampsBlobStore.getBlobPathForUpload(remotePinnedTimestamps),
                        oldFilesToBeDeleted
                    );
                } catch (IOException e) {
                    logger.error("Exception while deleting stale pinned timestamps", e);
                }
            }
            listener.onResponse(null);
        }, listener::onFailure);
    }

    private PinnedTimestamps readExistingPinnedTimestamps(String blobFilename, RemotePinnedTimestamps remotePinnedTimestamps) {
        remotePinnedTimestamps.setBlobFileName(blobFilename);
        remotePinnedTimestamps.setFullBlobName(pinnedTimestampsBlobStore.getBlobPathForUpload(remotePinnedTimestamps));
        try {
            return pinnedTimestampsBlobStore.read(remotePinnedTimestamps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read existing pinned timestamps", e);
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
            RemotePinnedTimestamps remotePinnedTimestamps = new RemotePinnedTimestamps(
                clusterService.state().metadata().clusterUUID(),
                blobStoreRepository.getCompressor()
            );
            BlobPath path = pinnedTimestampsBlobStore.getBlobPathForUpload(remotePinnedTimestamps);
            blobStoreTransferService.listAllInSortedOrder(path, remotePinnedTimestamps.getType(), 1, new ActionListener<>() {
                @Override
                public void onResponse(List<BlobMetadata> blobMetadata) {
                    if (blobMetadata.isEmpty()) {
                        return;
                    }
                    PinnedTimestamps pinnedTimestamps = readExistingPinnedTimestamps(blobMetadata.get(0).name(), remotePinnedTimestamps);
                    logger.debug(
                        "Fetched pinned timestamps from remote store: {} - {}",
                        triggerTimestamp,
                        pinnedTimestamps.getPinnedTimestampPinningEntityMap().keySet()
                    );
                    pinnedTimestampsSet = new Tuple<>(triggerTimestamp, pinnedTimestamps.getPinnedTimestampPinningEntityMap().keySet());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Exception while listing pinned timestamp files", e);
                }
            });
        }
    }
}
