/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreCoreTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@LuceneTestCase.AwaitsFix(bugUrl = "Flakiness seen for this class")
public class S3RemoteStoreIT extends RemoteStoreCoreTestCase {

    @Override
    @SuppressForbidden(reason = "Need to set system property here for AWS SDK v2")
    public void setUp() throws Exception {
        SocketAccess.doPrivileged(() -> System.setProperty("opensearch.path.conf", "config"));
        super.setUp();
    }

    @Override
    @SuppressForbidden(reason = "Need to reset system property here for AWS SDK v2")
    public void tearDown() throws Exception {
        SocketAccess.doPrivileged(() -> System.clearProperty("opensearch.path.conf"));
        clearIndices();
        waitForEmptyRemotePurgeQueue();
        super.tearDown();
    }

    private void clearIndices() throws Exception {
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest("*")).get());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(S3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).setSecureSettings(credentials()).build();
    }

    private SecureSettings credentials() {
        assertFalse(Strings.isNullOrEmpty(System.getProperty("test.s3.account")));
        assertFalse(Strings.isNullOrEmpty(System.getProperty("test.s3.key")));
        assertFalse(Strings.isNullOrEmpty(System.getProperty("test.s3.bucket")));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", System.getProperty("test.s3.account"));
        secureSettings.setString("s3.client.default.secret_key", System.getProperty("test.s3.key"));
        return secureSettings;
    }

    @Override
    protected Settings remoteStoreRepoSettings() {

        String segmentRepoName = REPOSITORY_NAME;
        String translogRepoName = REPOSITORY_2_NAME;
        String stateRepoName = REPOSITORY_3_NAME;
        String segmentRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            segmentRepoName
        );
        String segmentRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            segmentRepoName
        );
        String translogRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            translogRepoName
        );
        String translogRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            translogRepoName
        );
        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            stateRepoName
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            stateRepoName
        );

        String prefixModeVerificationSuffix = BlobStoreRepository.PREFIX_MODE_VERIFICATION_SETTING.getKey();

        String bucket = System.getProperty("test.s3.bucket");
        String region = System.getProperty("test.s3.region", "us-west-2");
        String basePath = System.getProperty("test.s3.base", "testpath");
        String segmentBasePath = basePath + "-segments";
        String translogBasePath = basePath + "-translog";
        String stateBasePath = basePath + "-state";

        Settings.Builder settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, segmentRepoName)
            .put(segmentRepoTypeAttributeKey, S3Repository.TYPE)
            .put(segmentRepoSettingsAttributeKeyPrefix + "bucket", bucket)
            .put(segmentRepoSettingsAttributeKeyPrefix + "region", region)
            .put(segmentRepoSettingsAttributeKeyPrefix + "base_path", segmentBasePath)
            .put(segmentRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, prefixModeVerificationEnable)
            .put("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, translogRepoName)
            .put(translogRepoTypeAttributeKey, S3Repository.TYPE)
            .put(translogRepoSettingsAttributeKeyPrefix + "bucket", bucket)
            .put(translogRepoSettingsAttributeKeyPrefix + "region", region)
            .put(translogRepoSettingsAttributeKeyPrefix + "base_path", translogBasePath)
            .put(translogRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, prefixModeVerificationEnable)
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, stateRepoName)
            .put(stateRepoTypeAttributeKey, S3Repository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "bucket", bucket)
            .put(stateRepoSettingsAttributeKeyPrefix + "region", region)
            .put(stateRepoSettingsAttributeKeyPrefix + "base_path", stateBasePath)
            .put(stateRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, prefixModeVerificationEnable);

        final String endpoint = System.getProperty("test.s3.endpoint");
        if (endpoint != null) {
            settings.put(segmentRepoSettingsAttributeKeyPrefix + "endpoint", endpoint);
            settings.put(translogRepoSettingsAttributeKeyPrefix + "endpoint", endpoint);
            settings.put(stateRepoSettingsAttributeKeyPrefix + "endpoint", endpoint);
        }

        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(RemoteStoreEnums.PathType.values()));
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), randomBoolean());
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), randomBoolean());
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.getKey(), segmentsPathFixedPrefix ? "a" : "");
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.getKey(), translogPathFixedPrefix ? "b" : "");
        settings.put(BlobStoreRepository.SNAPSHOT_SHARD_PATH_PREFIX_SETTING.getKey(), snapshotShardPathFixedPrefix ? "c" : "");

        return settings.build();
    }

    @Override
    @AwaitsFix(bugUrl = "assertion of cluster health timeout trips")
    public void testNoMultipleWriterDuringPrimaryRelocation() throws ExecutionException, InterruptedException {
        super.testNoMultipleWriterDuringPrimaryRelocation();
    }

    @Override
    @AwaitsFix(bugUrl = "assertion of cluster health timeout trips")
    public void testResumeUploadAfterFailedPrimaryRelocation() throws ExecutionException, InterruptedException, IOException {
        super.testResumeUploadAfterFailedPrimaryRelocation();
    }

    @Override
    @AwaitsFix(bugUrl = "Test times out due to too many translog upload")
    public void testFlushOnTooManyRemoteTranslogFiles() throws Exception {
        super.testFlushOnTooManyRemoteTranslogFiles();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    protected BlobStoreRepository getRepository() {
        return (BlobStoreRepository) internalCluster().getDataNodeInstance(RepositoriesService.class).repository(REPOSITORY_2_NAME);
    }

    @Override
    protected int getActualFileCount(Path ignoredSegmentRepoPath, String shardPath) throws IOException {
        BlobStoreRepository repository = getRepository();
        return repository.blobStore().blobContainer(BlobPath.cleanPath().add(shardPath)).listBlobs().size();
    }

    @Override
    protected void delete(Path baseRepoPath, String shardPath) throws IOException {
        BlobStoreRepository repository = getRepository();
        repository.blobStore().blobContainer(repository.basePath().add(shardPath)).delete();
    }

    private void waitForEmptyRemotePurgeQueue() throws Exception {
        if (internalCluster().getDataNodeNames().isEmpty()) {
            return;
        }
        assertBusyWithFixedSleepTime(() -> {
            ThreadPoolStats.Stats remotePurgeThreadPoolStats = getRemotePurgeThreadPoolStats();
            assertEquals(0, remotePurgeThreadPoolStats.getQueue());
            assertEquals(0, remotePurgeThreadPoolStats.getQueue());
        }, TimeValue.timeValueSeconds(60), TimeValue.timeValueMillis(500));
    }

    ThreadPoolStats.Stats getRemotePurgeThreadPoolStats() {
        final ThreadPoolStats stats = internalCluster().getDataNodeInstance(ThreadPool.class).stats();
        for (ThreadPoolStats.Stats s : stats) {
            if (s.getName().equals(ThreadPool.Names.REMOTE_PURGE)) {
                return s;
            }
        }
        throw new AssertionError("refresh thread pool stats not found [" + stats + "]");
    }

    @Override
    protected BlobPath getSegmentBasePath() {
        String basePath = System.getProperty("test.s3.base", "testpath");
        String segmentBasePath = basePath + "-segments";
        return BlobPath.cleanPath().add(segmentBasePath);
    }
}
