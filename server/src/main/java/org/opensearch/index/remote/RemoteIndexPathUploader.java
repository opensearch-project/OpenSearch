/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.IndexMetadataUploadListener;
import org.opensearch.gateway.remote.RemoteClusterStateService.RemoteStateTransferException;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ConfigBlobStoreFormat;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.gateway.remote.RemoteClusterStateService.INDEX_METADATA_UPLOAD_TIMEOUT_SETTING;
import static org.opensearch.index.remote.RemoteIndexPath.COMBINED_PATH;
import static org.opensearch.index.remote.RemoteIndexPath.SEGMENT_PATH;
import static org.opensearch.index.remote.RemoteIndexPath.TRANSLOG_PATH;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteDataAttributePresent;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;

/**
 * Uploads the remote store path for all possible combinations of {@link org.opensearch.index.remote.RemoteStoreEnums.DataCategory}
 * and {@link org.opensearch.index.remote.RemoteStoreEnums.DataType} for each shard of an index.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class RemoteIndexPathUploader extends IndexMetadataUploadListener {

    public static final ConfigBlobStoreFormat<RemoteIndexPath> REMOTE_INDEX_PATH_FORMAT = new ConfigBlobStoreFormat<>(
        RemoteIndexPath.FILE_NAME_FORMAT
    );

    private static final String TIMEOUT_EXCEPTION_MSG = "Timed out waiting while uploading remote index path file for indexes=%s";
    private static final String UPLOAD_EXCEPTION_MSG = "Exception occurred while uploading remote index paths for indexes=%s";
    static final String TRANSLOG_REPO_NAME_KEY = Node.NODE_ATTRIBUTES.getKey()
        + RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
    static final String SEGMENT_REPO_NAME_KEY = Node.NODE_ATTRIBUTES.getKey()
        + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;

    private static final Logger logger = LogManager.getLogger(RemoteIndexPathUploader.class);

    private final Settings settings;
    private final boolean isRemoteDataAttributePresent;
    private final boolean isTranslogSegmentRepoSame;
    private final Supplier<RepositoriesService> repositoriesService;
    private volatile TimeValue indexMetadataUploadTimeout;

    private BlobStoreRepository translogRepository;
    private BlobStoreRepository segmentRepository;

    public RemoteIndexPathUploader(
        ThreadPool threadPool,
        Settings settings,
        Supplier<RepositoriesService> repositoriesService,
        ClusterSettings clusterSettings
    ) {
        super(threadPool, ThreadPool.Names.GENERIC);
        this.settings = Objects.requireNonNull(settings);
        this.repositoriesService = Objects.requireNonNull(repositoriesService);
        isRemoteDataAttributePresent = isRemoteDataAttributePresent(settings);
        // If the remote data attributes are not present, then there is no effect of translog and segment being same or different or null.
        isTranslogSegmentRepoSame = isTranslogSegmentRepoSame();
        Objects.requireNonNull(clusterSettings);
        indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
    }

    @Override
    protected void doOnNewIndexUpload(List<IndexMetadata> indexMetadataList, ActionListener<Void> actionListener) {
        if (isRemoteDataAttributePresent == false) {
            logger.trace("Skipping beforeNewIndexUpload as there are no remote indexes");
            actionListener.onResponse(null);
            return;
        }

        long startTime = System.nanoTime();
        boolean success = false;
        List<IndexMetadata> eligibleList = indexMetadataList.stream().filter(this::requiresPathUpload).collect(Collectors.toList());
        String indexNames = eligibleList.stream().map(IndexMetadata::getIndex).map(Index::toString).collect(Collectors.joining(","));
        int latchCount = eligibleList.size() * (isTranslogSegmentRepoSame ? 1 : 2);
        CountDownLatch latch = new CountDownLatch(latchCount);
        List<Exception> exceptionList = Collections.synchronizedList(new ArrayList<>(latchCount));
        try {
            for (IndexMetadata indexMetadata : eligibleList) {
                writeIndexPathAsync(indexMetadata, latch, exceptionList);
            }

            logger.trace(new ParameterizedMessage("Remote index path upload started for {}", indexNames));

            try {
                if (latch.await(indexMetadataUploadTimeout.millis(), TimeUnit.MILLISECONDS) == false) {
                    RemoteStateTransferException ex = new RemoteStateTransferException(
                        String.format(Locale.ROOT, TIMEOUT_EXCEPTION_MSG, indexNames)
                    );
                    exceptionList.forEach(ex::addSuppressed);
                    actionListener.onFailure(ex);
                    return;
                }
            } catch (InterruptedException exception) {
                exceptionList.forEach(exception::addSuppressed);
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, TIMEOUT_EXCEPTION_MSG, indexNames),
                    exception
                );
                actionListener.onFailure(ex);
                return;
            }
            if (exceptionList.size() > 0) {
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, UPLOAD_EXCEPTION_MSG, indexNames)
                );
                exceptionList.forEach(ex::addSuppressed);
                actionListener.onFailure(ex);
                return;
            }
            success = true;
            actionListener.onResponse(null);
        } catch (Exception exception) {
            RemoteStateTransferException ex = new RemoteStateTransferException(
                String.format(Locale.ROOT, UPLOAD_EXCEPTION_MSG, indexNames),
                exception
            );
            exceptionList.forEach(ex::addSuppressed);
            actionListener.onFailure(ex);
        } finally {
            long tookTimeNs = System.nanoTime() - startTime;
            logger.trace(new ParameterizedMessage("executed beforeNewIndexUpload status={} tookTimeNs={}", success, tookTimeNs));
        }

    }

    private void writeIndexPathAsync(IndexMetadata idxMD, CountDownLatch latch, List<Exception> exceptionList) {
        if (isTranslogSegmentRepoSame) {
            // If the repositories are same, then we need to upload a single file containing paths for both translog and segments.
            writePathToRemoteStore(idxMD, translogRepository, latch, exceptionList, COMBINED_PATH);
        } else {
            // If the repositories are different, then we need to upload one file per segment and translog containing their individual
            // paths.
            writePathToRemoteStore(idxMD, translogRepository, latch, exceptionList, TRANSLOG_PATH);
            writePathToRemoteStore(idxMD, segmentRepository, latch, exceptionList, SEGMENT_PATH);
        }
    }

    private void writePathToRemoteStore(
        IndexMetadata idxMD,
        BlobStoreRepository repository,
        CountDownLatch latch,
        List<Exception> exceptionList,
        Map<RemoteStoreEnums.DataCategory, List<RemoteStoreEnums.DataType>> pathCreationMap
    ) {
        Map<String, String> remoteCustomData = idxMD.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        RemoteStoreEnums.PathType pathType = RemoteStoreEnums.PathType.valueOf(remoteCustomData.get(RemoteStoreEnums.PathType.NAME));
        RemoteStoreEnums.PathHashAlgorithm hashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.valueOf(
            remoteCustomData.get(RemoteStoreEnums.PathHashAlgorithm.NAME)
        );
        String indexUUID = idxMD.getIndexUUID();
        int shardCount = idxMD.getNumberOfShards();
        BlobPath basePath = repository.basePath();
        BlobContainer blobContainer = repository.blobStore().blobContainer(basePath.add(RemoteIndexPath.DIR));
        ActionListener<Void> actionListener = getUploadPathLatchedActionListener(idxMD, latch, exceptionList, pathCreationMap);
        try {
            REMOTE_INDEX_PATH_FORMAT.writeAsyncWithUrgentPriority(
                new RemoteIndexPath(indexUUID, shardCount, basePath, pathType, hashAlgorithm, pathCreationMap),
                blobContainer,
                indexUUID,
                actionListener
            );
        } catch (IOException ioException) {
            RemoteStateTransferException ex = new RemoteStateTransferException(
                String.format(Locale.ROOT, UPLOAD_EXCEPTION_MSG, List.of(idxMD.getIndex().getName()))
            );
            actionListener.onFailure(ioException);
        }
    }

    private Repository validateAndGetRepository(String repoSetting) {
        final String repo = settings.get(repoSetting);
        assert repo != null : "Remote " + repoSetting + " repository is not configured";
        final Repository repository = repositoriesService.get().repository(repo);
        assert repository instanceof BlobStoreRepository : "Repository should be instance of BlobStoreRepository";
        return repository;
    }

    public void start() {
        assert isRemoteStoreClusterStateEnabled(settings) == true : "Remote cluster state is not enabled";
        if (isRemoteDataAttributePresent == false) {
            // If remote store data attributes are not present than we skip this.
            return;
        }
        translogRepository = (BlobStoreRepository) validateAndGetRepository(TRANSLOG_REPO_NAME_KEY);
        segmentRepository = (BlobStoreRepository) validateAndGetRepository(SEGMENT_REPO_NAME_KEY);
    }

    private boolean isTranslogSegmentRepoSame() {
        String translogRepoName = settings.get(TRANSLOG_REPO_NAME_KEY);
        String segmentRepoName = settings.get(SEGMENT_REPO_NAME_KEY);
        return Objects.equals(translogRepoName, segmentRepoName);
    }

    private LatchedActionListener<Void> getUploadPathLatchedActionListener(
        IndexMetadata indexMetadata,
        CountDownLatch latch,
        List<Exception> exceptionList,
        Map<RemoteStoreEnums.DataCategory, List<RemoteStoreEnums.DataType>> pathCreationMap
    ) {
        return new LatchedActionListener<>(
            ActionListener.wrap(
                resp -> logger.trace(
                    new ParameterizedMessage("Index path uploaded for {} indexMetadata={}", pathCreationMap, indexMetadata)
                ),
                ex -> {
                    logger.error(
                        new ParameterizedMessage(
                            "Exception during Index path upload for {} indexMetadata={}",
                            pathCreationMap,
                            indexMetadata
                        ),
                        ex
                    );
                    exceptionList.add(ex);
                }
            ),
            latch
        );
    }

    /**
     * This method checks if the index metadata has attributes that calls for uploading the index path for remote store
     * uploads. It checks if the remote store path type is {@code HASHED_PREFIX} and returns true if so.
     */
    private boolean requiresPathUpload(IndexMetadata indexMetadata) {
        // A cluster will have remote custom metadata only if the cluster is remote store enabled from data side.
        Map<String, String> remoteCustomData = indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        if (Objects.isNull(remoteCustomData) || remoteCustomData.isEmpty()) {
            return false;
        }
        String pathTypeStr = remoteCustomData.get(RemoteStoreEnums.PathType.NAME);
        if (Objects.isNull(pathTypeStr)) {
            return false;
        }
        // We need to upload the path only if the path type for an index is hashed_prefix
        return RemoteStoreEnums.PathType.HASHED_PREFIX == RemoteStoreEnums.PathType.parseString(pathTypeStr);
    }

    private void setIndexMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = newIndexMetadataUploadTimeout;
    }
}
