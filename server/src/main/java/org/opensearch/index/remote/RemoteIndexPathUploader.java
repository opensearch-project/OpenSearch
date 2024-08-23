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
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.IndexMetadataUploadListener;
import org.opensearch.gateway.remote.RemoteStateTransferException;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
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

import static org.opensearch.gateway.remote.RemoteGlobalMetadataManager.GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING;
import static org.opensearch.index.remote.RemoteIndexPath.COMBINED_PATH;
import static org.opensearch.index.remote.RemoteIndexPath.SEGMENT_PATH;
import static org.opensearch.index.remote.RemoteIndexPath.TRANSLOG_PATH;
import static org.opensearch.index.remote.RemoteStoreUtils.determineRemoteStorePathStrategy;
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

    public static final String DELIMITER = "#";
    public static final ConfigBlobStoreFormat<RemoteIndexPath> REMOTE_INDEX_PATH_FORMAT = new ConfigBlobStoreFormat<>(
        RemoteIndexPath.FILE_NAME_FORMAT,
        RemoteIndexPath::fromXContent
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
    private volatile TimeValue metadataUploadTimeout;

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
        metadataUploadTimeout = clusterSettings.get(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(GLOBAL_METADATA_UPLOAD_TIMEOUT_SETTING, this::setMetadataUploadTimeout);
    }

    @Override
    protected void doOnUpload(
        List<IndexMetadata> indexMetadataList,
        Map<String, IndexMetadata> prevIndexMetadataByName,
        ActionListener<Void> actionListener
    ) {
        if (isRemoteDataAttributePresent == false) {
            logger.trace("Skipping beforeNewIndexUpload as there are no remote indexes");
            actionListener.onResponse(null);
            return;
        }

        long startTime = System.nanoTime();
        boolean success = false;
        List<IndexMetadata> eligibleList = indexMetadataList.stream()
            .filter(idxMd -> requiresPathUpload(idxMd, prevIndexMetadataByName.get(idxMd.getIndex().getName())))
            .collect(Collectors.toList());
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
                if (latch.await(metadataUploadTimeout.millis(), TimeUnit.MILLISECONDS) == false) {
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
        PathType pathType = PathType.valueOf(remoteCustomData.get(PathType.NAME));
        RemoteStoreEnums.PathHashAlgorithm hashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.valueOf(
            remoteCustomData.get(RemoteStoreEnums.PathHashAlgorithm.NAME)
        );
        String indexUUID = idxMD.getIndexUUID();
        int shardCount = idxMD.getNumberOfShards();
        BlobPath basePath = repository.basePath();
        BlobContainer blobContainer = repository.blobStore().blobContainer(basePath.add(RemoteIndexPath.DIR));
        ActionListener<Void> actionListener = getUploadPathLatchedActionListener(idxMD, latch, exceptionList, pathCreationMap);
        try {
            RemoteIndexPath remoteIndexPath = new RemoteIndexPath(
                indexUUID,
                shardCount,
                basePath,
                pathType,
                hashAlgorithm,
                pathCreationMap
            );
            String fileName = generateFileName(indexUUID, idxMD.getVersion(), remoteIndexPath.getVersion());
            REMOTE_INDEX_PATH_FORMAT.writeAsyncWithUrgentPriority(remoteIndexPath, blobContainer, fileName, actionListener);
        } catch (IOException ioException) {
            RemoteStateTransferException ex = new RemoteStateTransferException(
                String.format(Locale.ROOT, UPLOAD_EXCEPTION_MSG, List.of(idxMD.getIndex().getName())),
                ioException
            );
            actionListener.onFailure(ex);
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
        // TODO - The current comparison checks the repository name. But it is also possible that the repository are same
        // by attributes, but different by name. We need to handle this.
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
    private boolean requiresPathUpload(IndexMetadata indexMetadata, IndexMetadata prevIndexMetadata) {
        PathType pathType = determineRemoteStorePathStrategy(indexMetadata).getType();
        PathType prevPathType = Objects.nonNull(prevIndexMetadata) ? determineRemoteStorePathStrategy(prevIndexMetadata).getType() : null;
        // If previous metadata is null or previous path type is not hashed_prefix, and along with new path type being
        // hashed_prefix, then this can mean any of the following -
        // 1. This is creation of remote index with hashed_prefix
        // 2. We are enabling cluster state for the very first time with multiple indexes having hashed_prefix path type.
        // 3. A docrep index is being migrated to being remote store index.
        return pathType == PathType.HASHED_PREFIX && (Objects.isNull(prevPathType) || prevPathType != PathType.HASHED_PREFIX);
    }

    private void setMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.metadataUploadTimeout = newIndexMetadataUploadTimeout;
    }

    /**
     * Creates a file name by combining index uuid, index metadata version and file version. # has been chosen as the
     * delimiter since it does not collide with any possible letters in file name. The random base64 uuid is added to
     * ensure that the file does not get overwritten. We do check if translog and segment repo are same by name, but
     * it is possible that a user configures same repo by different name for translog and segment in which case, this
     * will lead to file not being overwritten.
     */
    private String generateFileName(String indexUUID, long indexMetadataVersion, String fileVersion) {
        return String.join(DELIMITER, indexUUID, Long.toString(indexMetadataVersion), fileVersion, UUIDs.randomBase64UUID());
    }
}
