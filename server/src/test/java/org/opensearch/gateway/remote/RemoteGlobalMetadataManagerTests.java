/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.metadata.IndexGraveyard;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.Metadata.XContentContext;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.TestCustomMetadata;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteGlobalMetadataManagerTests extends OpenSearchTestCase {
    private RemoteGlobalMetadataManager remoteGlobalMetadataManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        BlobStoreTransferService blobStoreTransferService = mock(BlobStoreTransferService.class);
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
        Compressor compressor = new NoneCompressor();
        when(blobStoreRepository.getCompressor()).thenReturn(compressor);
        when(blobStoreRepository.getNamedXContentRegistry()).thenReturn(xContentRegistry);
        remoteGlobalMetadataManager = new RemoteGlobalMetadataManager(
            clusterSettings,
            "test-cluster",
            blobStoreRepository,
            blobStoreTransferService,
            writableRegistry(),
            threadPool
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGlobalMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteGlobalMetadataManager.GLOBAL_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout()
        );

        // verify update global metadata upload timeout
        int globalMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.global_metadata.upload_timeout", globalMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(globalMetadataUploadTimeout, remoteGlobalMetadataManager.getGlobalMetadataUploadTimeout().seconds());
    }

    public void testGetUpdatedCustoms() {
        Map<String, Metadata.Custom> previousCustoms = Map.of(
            CustomMetadata1.TYPE,
            new CustomMetadata1("data1"),
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3")
        );
        ClusterState previousState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(Metadata.builder().customs(previousCustoms))
            .build();

        Map<String, Metadata.Custom> currentCustoms = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5")
        );
        ClusterState currentState = ClusterState.builder(new ClusterName("test-cluster"))
            .metadata(Metadata.builder().customs(currentCustoms))
            .build();

        DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> customsDiff = remoteGlobalMetadataManager
            .getCustomsDiff(currentState, previousState, true, false);
        Map<String, Metadata.Custom> expectedUpserts = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            IndexGraveyard.TYPE,
            IndexGraveyard.builder().build()
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of()));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, false, false);
        expectedUpserts = Map.of(
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4")
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of(CustomMetadata1.TYPE)));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, true, true);
        expectedUpserts = Map.of(
            CustomMetadata2.TYPE,
            new CustomMetadata2("data2"),
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5"),
            IndexGraveyard.TYPE,
            IndexGraveyard.builder().build()
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of()));

        customsDiff = remoteGlobalMetadataManager.getCustomsDiff(currentState, previousState, false, true);
        expectedUpserts = Map.of(
            CustomMetadata3.TYPE,
            new CustomMetadata3("data3-changed"),
            CustomMetadata4.TYPE,
            new CustomMetadata4("data4"),
            CustomMetadata5.TYPE,
            new CustomMetadata5("data5")
        );
        assertThat(customsDiff.getUpserts(), is(expectedUpserts));
        assertThat(customsDiff.getDeletes(), is(List.of(CustomMetadata1.TYPE)));

    }

    private static class CustomMetadata1 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_1";

        CustomMetadata1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata2 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_2";

        CustomMetadata2(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata3 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_3";

        CustomMetadata3(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata4 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_4";

        CustomMetadata4(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetadata5 extends TestCustomMetadata {
        public static final String TYPE = "custom_md_5";

        CustomMetadata5(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(XContentContext.API);
        }
    }
}
