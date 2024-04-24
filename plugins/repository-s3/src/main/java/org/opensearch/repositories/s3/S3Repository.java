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

import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.StorageClass;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.opensearch.repositories.s3.async.AsyncExecutorContainer;
import org.opensearch.repositories.s3.async.AsyncTransferManager;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.threadpool.Scheduler;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code bucket}</dt><dd>S3 bucket</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt>
 * <dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to not chucked.</dd>
 * </dl>
 */
class S3Repository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(S3Repository.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    static final String TYPE = "s3";

    /** The access key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.insecureString("access_key");

    /** The secret key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.insecureString("secret_key");

    /**
     * Default is to use 100MB (S3 defaults) for heaps above 2GB and 5% of
     * the available memory for smaller heaps.
     */
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(ByteSizeUnit.MB.toBytes(100), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
        ),
        ByteSizeUnit.BYTES
    );

    private static final ByteSizeValue DEFAULT_MULTIPART_UPLOAD_MINIMUM_PART_SIZE = new ByteSizeValue(
        ByteSizeUnit.MB.toBytes(16),
        ByteSizeUnit.BYTES
    );

    static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");

    /**
     * When set to true files are encrypted on server side using AES256 algorithm.
     * Defaults to false.
     */
    static final Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING = Setting.boolSetting("server_side_encryption", false);

    /**
     * Maximum size of files that can be uploaded using a single upload request.
     */
    static final ByteSizeValue MAX_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    /**
     * Minimum size of parts that can be uploaded using the Multipart Upload API.
     * (see http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
     */
    static final ByteSizeValue MIN_PART_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.MB);

    /**
     * Maximum size of parts that can be uploaded using the Multipart Upload API.
     * (see http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
     */
    static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;

    /**
     * Maximum size of files that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);

    /**
     * Whether large uploads need to be redirected to slow sync s3 client.
     */
    static final Setting<Boolean> REDIRECT_LARGE_S3_UPLOAD = Setting.boolSetting(
        "redirect_large_s3_upload",
        true,
        Setting.Property.NodeScope
    );

    /**
     * Whether retry on uploads are enabled. This setting wraps inputstream with buffered stream to enable retries.
     */
    static final Setting<Boolean> UPLOAD_RETRY_ENABLED = Setting.boolSetting("s3_upload_retry_enabled", true, Setting.Property.NodeScope);

    /**
     * Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold,
     * the S3 repository will use the AWS Multipart Upload API to split the chunk into several parts, each of buffer_size length, and
     * to upload each part in its own request. Note that setting a buffer size lower than 5mb is not allowed since it will prevents the
     * use of the Multipart API and may result in upload errors. Defaults to the minimum between 100MB and 5% of the heap size.
     */
    static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "buffer_size",
        DEFAULT_BUFFER_SIZE,
        MIN_PART_SIZE_USING_MULTIPART,
        MAX_PART_SIZE_USING_MULTIPART
    );

    /**
     * Minimum part size for parallel multipart uploads
     */
    static final Setting<ByteSizeValue> PARALLEL_MULTIPART_UPLOAD_MINIMUM_PART_SIZE_SETTING = Setting.byteSizeSetting(
        "parallel_multipart_upload.minimum_part_size",
        DEFAULT_MULTIPART_UPLOAD_MINIMUM_PART_SIZE,
        MIN_PART_SIZE_USING_MULTIPART,
        MAX_PART_SIZE_USING_MULTIPART,
        Setting.Property.NodeScope
    );

    /**
     * This setting controls whether parallel multipart uploads will be used when calling S3 or not
     */
    public static Setting<Boolean> PARALLEL_MULTIPART_UPLOAD_ENABLED_SETTING = Setting.boolSetting(
        "parallel_multipart_upload.enabled",
        true,
        Setting.Property.NodeScope
    );
    /**
     * Percentage of total available permits to be available for priority transfers.
     */
    public static Setting<Integer> S3_PRIORITY_PERMIT_ALLOCATION_PERCENT = Setting.intSetting(
        "s3_priority_permit_alloc_perc",
        70,
        21,
        80,
        Setting.Property.NodeScope
    );

    /**
     * Duration in minutes to wait for a permit in case no permit is available.
     */
    public static Setting<Integer> S3_PERMIT_WAIT_DURATION_MIN = Setting.intSetting(
        "s3_permit_wait_duration_min",
        5,
        1,
        10,
        Setting.Property.NodeScope
    );

    /**
     * Number of transfer queue consumers
     */
    public static Setting<Integer> S3_TRANSFER_QUEUE_CONSUMERS = new Setting<>(
        "s3_transfer_queue_consumers",
        (s) -> Integer.toString(Math.max(5, OpenSearchExecutors.allocatedProcessors(s) * 2)),
        (s) -> Setting.parseInt(s, 5, "s3_transfer_queue_consumers"),
        Setting.Property.NodeScope
    );

    /**
     * Big files can be broken down into chunks during snapshotting if needed. Defaults to 1g.
     */
    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "chunk_size",
        new ByteSizeValue(1, ByteSizeUnit.GB),
        new ByteSizeValue(5, ByteSizeUnit.MB),
        new ByteSizeValue(5, ByteSizeUnit.TB)
    );

    /**
     * Maximum number of deletes in a DeleteObjectsRequest.
     *
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html">S3 Documentation</a>.
     */
    static final Setting<Integer> BULK_DELETE_SIZE = Setting.intSetting("bulk_delete_size", 1000, 1, 1000);

    /**
     * Sets the S3 storage class type for the backup files. Values may be standard, reduced_redundancy,
     * standard_ia, onezone_ia and intelligent_tiering. Defaults to standard.
     */
    static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    /**
     * The S3 repository supports all S3 canned ACLs : private, public-read, public-read-write,
     * authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control. Defaults to private.
     */
    static final Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");

    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    private final S3Service service;

    private volatile String bucket;

    private volatile ByteSizeValue bufferSize;

    private volatile ByteSizeValue chunkSize;

    private volatile BlobPath basePath;

    private volatile boolean serverSideEncryption;

    private volatile String storageClass;

    private volatile String cannedACL;
    private final AsyncTransferManager asyncUploadUtils;
    private final S3AsyncService s3AsyncService;
    private final boolean multipartUploadEnabled;
    private final AsyncExecutorContainer urgentExecutorBuilder;
    private final AsyncExecutorContainer priorityExecutorBuilder;
    private final AsyncExecutorContainer normalExecutorBuilder;
    private final Path pluginConfigPath;
    private final SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ;
    private final SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ;

    private volatile int bulkDeletesSize;

    // Used by test classes
    S3Repository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final S3Service service,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings,
        final AsyncTransferManager asyncUploadUtils,
        final AsyncExecutorContainer urgentExecutorBuilder,
        final AsyncExecutorContainer priorityExecutorBuilder,
        final AsyncExecutorContainer normalExecutorBuilder,
        final S3AsyncService s3AsyncService,
        final boolean multipartUploadEnabled,
        final SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ,
        final SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ
    ) {
        this(
            metadata,
            namedXContentRegistry,
            service,
            clusterService,
            recoverySettings,
            asyncUploadUtils,
            urgentExecutorBuilder,
            priorityExecutorBuilder,
            normalExecutorBuilder,
            s3AsyncService,
            multipartUploadEnabled,
            Path.of(""),
            normalPrioritySizeBasedBlockingQ,
            lowPrioritySizeBasedBlockingQ
        );
    }

    /**
     * Constructs an s3 backed repository
     */
    S3Repository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final S3Service service,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings,
        final AsyncTransferManager asyncUploadUtils,
        final AsyncExecutorContainer urgentExecutorBuilder,
        final AsyncExecutorContainer priorityExecutorBuilder,
        final AsyncExecutorContainer normalExecutorBuilder,
        final S3AsyncService s3AsyncService,
        final boolean multipartUploadEnabled,
        Path pluginConfigPath,
        final SizeBasedBlockingQ normalPrioritySizeBasedBlockingQ,
        final SizeBasedBlockingQ lowPrioritySizeBasedBlockingQ
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings, buildLocation(metadata));
        this.service = service;
        this.s3AsyncService = s3AsyncService;
        this.multipartUploadEnabled = multipartUploadEnabled;
        this.pluginConfigPath = pluginConfigPath;
        this.asyncUploadUtils = asyncUploadUtils;
        this.urgentExecutorBuilder = urgentExecutorBuilder;
        this.priorityExecutorBuilder = priorityExecutorBuilder;
        this.normalExecutorBuilder = normalExecutorBuilder;
        this.normalPrioritySizeBasedBlockingQ = normalPrioritySizeBasedBlockingQ;
        this.lowPrioritySizeBasedBlockingQ = lowPrioritySizeBasedBlockingQ;

        validateRepositoryMetadata(metadata);
        readRepositoryMetadata();
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of("base_path", BASE_PATH_SETTING.get(metadata.settings()), "bucket", BUCKET_SETTING.get(metadata.settings()));
    }

    /**
     * Holds a reference to delayed repository operation {@link Scheduler.Cancellable} so it can be cancelled should the repository be
     * closed concurrently.
     */
    private final AtomicReference<Scheduler.Cancellable> finalizationFuture = new AtomicReference<>();

    @Override
    public void finalizeSnapshot(
        ShardGenerations shardGenerations,
        long repositoryStateId,
        Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        ActionListener<RepositoryData> listener
    ) {
        super.finalizeSnapshot(
            shardGenerations,
            repositoryStateId,
            clusterMetadata,
            snapshotInfo,
            repositoryMetaVersion,
            stateTransformer,
            listener
        );
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        ActionListener<RepositoryData> listener
    ) {
        super.deleteSnapshots(snapshotIds, repositoryStateId, repositoryMetaVersion, listener);
    }

    @Override
    protected S3BlobStore createBlobStore() {
        return new S3BlobStore(
            service,
            s3AsyncService,
            multipartUploadEnabled,
            bucket,
            serverSideEncryption,
            bufferSize,
            cannedACL,
            storageClass,
            bulkDeletesSize,
            metadata,
            asyncUploadUtils,
            urgentExecutorBuilder,
            priorityExecutorBuilder,
            normalExecutorBuilder,
            normalPrioritySizeBasedBlockingQ,
            lowPrioritySizeBasedBlockingQ
        );
    }

    // only use for testing (S3RepositoryTests)
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    public boolean isReloadable() {
        return true;
    }

    @Override
    public void reload(RepositoryMetadata newRepositoryMetadata) {
        if (isReloadable() == false) {
            return;
        }

        // Reload configs for S3Repository
        super.reload(newRepositoryMetadata);
        readRepositoryMetadata();

        // Reload configs for S3RepositoryPlugin
        service.settings(metadata);
        service.releaseCachedClients();
        s3AsyncService.settings(metadata);
        s3AsyncService.releaseCachedClients();

        // Reload configs for S3BlobStore
        BlobStore blobStore = getBlobStore();
        blobStore.reload(metadata);
    }

    /**
     * Reloads the values derived from the Repository Metadata
     */
    private void readRepositoryMetadata() {
        this.bucket = BUCKET_SETTING.get(metadata.settings());
        this.bufferSize = BUFFER_SIZE_SETTING.get(metadata.settings());
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }

        this.serverSideEncryption = SERVER_SIDE_ENCRYPTION_SETTING.get(metadata.settings());
        this.storageClass = STORAGE_CLASS_SETTING.get(metadata.settings());
        this.cannedACL = CANNED_ACL_SETTING.get(metadata.settings());
        this.bulkDeletesSize = BULK_DELETE_SIZE.get(metadata.settings());
        if (S3ClientSettings.checkDeprecatedCredentials(metadata.settings())) {
            // provided repository settings
            deprecationLogger.deprecate(
                "s3_repository_secret_settings",
                "Using s3 access/secret key from repository settings. Instead "
                    + "store these in named clients and the opensearch keystore for secure settings."
            );
        }

        logger.debug(
            "using bucket [{}], chunk_size [{}], server_side_encryption [{}], buffer_size [{}], cannedACL [{}], storageClass [{}]",
            bucket,
            chunkSize,
            serverSideEncryption,
            bufferSize,
            cannedACL,
            storageClass
        );
    }

    @Override
    public void validateMetadata(RepositoryMetadata newRepositoryMetadata) {
        super.validateMetadata(newRepositoryMetadata);
        validateRepositoryMetadata(newRepositoryMetadata);
    }

    private void validateRepositoryMetadata(RepositoryMetadata newRepositoryMetadata) {
        Settings settings = newRepositoryMetadata.settings();
        if (BUCKET_SETTING.get(settings) == null) {
            throw new RepositoryException(newRepositoryMetadata.name(), "No bucket defined for s3 repository");
        }

        // We make sure that chunkSize is bigger or equal than/to bufferSize
        if (CHUNK_SIZE_SETTING.get(settings).getBytes() < BUFFER_SIZE_SETTING.get(settings).getBytes()) {
            throw new RepositoryException(
                newRepositoryMetadata.name(),
                CHUNK_SIZE_SETTING.getKey()
                    + " ("
                    + CHUNK_SIZE_SETTING.get(settings)
                    + ") can't be lower than "
                    + BUFFER_SIZE_SETTING.getKey()
                    + " ("
                    + BUFFER_SIZE_SETTING.get(settings)
                    + ")."
            );
        }

        validateStorageClass(STORAGE_CLASS_SETTING.get(settings));
        validateCannedACL(CANNED_ACL_SETTING.get(settings));
    }

    private static void validateStorageClass(String storageClassStringValue) {
        if ((storageClassStringValue == null) || storageClassStringValue.equals("")) {
            return;
        }

        final StorageClass storageClass = StorageClass.fromValue(storageClassStringValue.toUpperCase(Locale.ENGLISH));
        if (storageClass.equals(StorageClass.GLACIER)) {
            throw new BlobStoreException("Glacier storage class is not supported");
        }

        if (storageClass == StorageClass.UNKNOWN_TO_SDK_VERSION) {
            throw new BlobStoreException("`" + storageClassStringValue + "` is not a valid S3 Storage Class.");
        }
    }

    private static void validateCannedACL(String cannedACLStringValue) {
        if ((cannedACLStringValue == null) || cannedACLStringValue.equals("")) {
            return;
        }

        for (final ObjectCannedACL cur : ObjectCannedACL.values()) {
            if (cur.toString().equalsIgnoreCase(cannedACLStringValue)) {
                return;
            }
        }

        throw new BlobStoreException("cannedACL is not valid: [" + cannedACLStringValue + "]");
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public List<Setting<?>> getRestrictedSystemRepositorySettings() {
        List<Setting<?>> restrictedSettings = new ArrayList<>();
        restrictedSettings.addAll(super.getRestrictedSystemRepositorySettings());
        restrictedSettings.add(BUCKET_SETTING);
        restrictedSettings.add(BASE_PATH_SETTING);
        return restrictedSettings;
    }

    @Override
    protected void doClose() {
        final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
        if (cancellable != null) {
            logger.debug("Repository [{}] closed during cool-down period", metadata.name());
            cancellable.cancel();
        }
        super.doClose();
    }
}
