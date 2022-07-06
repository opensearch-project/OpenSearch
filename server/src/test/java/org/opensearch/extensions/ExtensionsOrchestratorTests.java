/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterSettingsResponse;
import org.opensearch.cluster.LocalNodeResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class ExtensionsOrchestratorTests extends OpenSearchTestCase {

    private TransportService transportService;
    private final ThreadPool threadPool = new TestThreadPool(ExtensionsOrchestratorTests.class.getSimpleName());
    private final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        MockNioTransport transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()
        );
        transportService = new MockTransportService(
            settings,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (boundAddress) -> new DiscoveryNode(
                "test_node",
                "test_node",
                boundAddress.publishAddress(),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            ),
            null,
            Collections.emptySet()
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testExtensionsDiscovery() throws Exception {
        Path extensionDir = createTempDir();

        List<String> extensionsYmlLines = Arrays.asList(
            "extensions:",
            "   - name: firstExtension",
            "     uniqueId: uniqueid1",
            "     hostName: 'myIndependentPluginHost1'",
            "     hostAddress: '127.0.0.0'",
            "     port: '9300'",
            "     version: '0.0.7'",
            "     description: Fake description 1",
            "     opensearchVersion: '3.0.0'",
            "     javaVersion: '14'",
            "     className: fakeClass1",
            "     customFolderName: fakeFolder1",
            "     hasNativeController: false",
            "   - name: secondExtension",
            "     uniqueId: 'uniqueid2'",
            "     hostName: 'myIndependentPluginHost2'",
            "     hostAddress: '127.0.0.1'",
            "     port: '9301'",
            "     version: '3.14.16'",
            "     description: Fake description 2",
            "     opensearchVersion: '2.0.0'",
            "     javaVersion: '17'",
            "     className: fakeClass2",
            "     customFolderName: fakeFolder2",
            "     hasNativeController: true"
        );
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        ExtensionsOrchestrator extensionsOrchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        List<DiscoveryExtension> expectedExtensionsList = new ArrayList<DiscoveryExtension>();

        expectedExtensionsList.add(
            new DiscoveryExtension(
                "firstExtension",
                "uniqueid1",
                "uniqueid1",
                "myIndependentPluginHost1",
                "127.0.0.0",
                new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
                new HashMap<String, String>(),
                Version.fromString("3.0.0"),
                new PluginInfo(
                    "firstExtension",
                    "Fake description 1",
                    "0.0.7",
                    Version.fromString("3.0.0"),
                    "14",
                    "fakeClass1",
                    new ArrayList<String>(),
                    false
                )
            )
        );

        expectedExtensionsList.add(
            new DiscoveryExtension(
                "secondExtension",
                "uniqueid2",
                "uniqueid2",
                "myIndependentPluginHost2",
                "127.0.0.1",
                new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                new HashMap<String, String>(),
                Version.fromString("2.0.0"),
                new PluginInfo(
                    "secondExtension",
                    "Fake description 2",
                    "3.14.16",
                    Version.fromString("2.0.0"),
                    "17",
                    "fakeClass2",
                    new ArrayList<String>(),
                    true
                )
            )
        );
        assertEquals(expectedExtensionsList, extensionsOrchestrator.extensionsList);
    }

    public void testNonAccessibleDirectory() throws Exception {
        AccessControlException e = expectThrows(

            AccessControlException.class,
            () -> new ExtensionsOrchestrator(settings, PathUtils.get(""))
        );
        assertEquals("access denied (\"java.io.FilePermission\" \"\" \"read\")", e.getMessage());
    }

    public void testNoExtensionsFile() throws Exception {
        Path extensionDir = createTempDir();

        Settings settings = Settings.builder().build();

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsOrchestrator.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "No Extensions File Present",
                    "org.opensearch.extensions.ExtensionsOrchestrator",
                    Level.INFO,
                    "Extensions.yml file is not present.  No extensions will be loaded."
                )
            );

            new ExtensionsOrchestrator(settings, extensionDir);

            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testEmptyExtensionsFile() throws Exception {
        Path extensionDir = createTempDir();

        List<String> extensionsYmlLines = Arrays.asList();
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        Settings settings = Settings.builder().build();

        expectThrows(IOException.class, () -> new ExtensionsOrchestrator(settings, extensionDir));
    }

    public void testExtensionsInitialize() throws Exception {
        Path extensionDir = createTempDir();

        List<String> extensionsYmlLines = Arrays.asList(
            "extensions:",
            "   - name: firstExtension",
            "     uniqueId: uniqueid1",
            "     hostName: 'myIndependentPluginHost1'",
            "     hostAddress: '127.0.0.0'",
            "     port: '9300'",
            "     version: '0.0.7'",
            "     description: Fake description 1",
            "     opensearchVersion: '3.0.0'",
            "     javaVersion: '14'",
            "     className: fakeClass1",
            "     customFolderName: fakeFolder1",
            "     hasNativeController: false",
            "   - name: secondExtension",
            "     uniqueId: 'uniqueid2'",
            "     hostName: 'myIndependentPluginHost2'",
            "     hostAddress: '127.0.0.1'",
            "     port: '9301'",
            "     version: '3.14.16'",
            "     description: Fake description 2",
            "     opensearchVersion: '2.0.0'",
            "     javaVersion: '17'",
            "     className: fakeClass2",
            "     customFolderName: fakeFolder2",
            "     hasNativeController: true"
        );
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        ExtensionsOrchestrator extensionsOrchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        transportService.start();
        transportService.acceptIncomingRequests();
        extensionsOrchestrator.setTransportService(transportService);

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsOrchestrator.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Connect Transport Exception 1",
                    "org.opensearch.extensions.ExtensionsOrchestrator",
                    Level.ERROR,
                    "ConnectTransportException[[firstExtension][127.0.0.0:9300] connect_timeout[30s]]"
                )
            );

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Connect Transport Exception 2",
                    "org.opensearch.extensions.ExtensionsOrchestrator",
                    Level.ERROR,
                    "ConnectTransportException[[secondExtension][127.0.0.1:9301] connect_exception]; nested: ConnectException[Connection refused];"
                )
            );

            extensionsOrchestrator.extensionsInitialize();
            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testHandleExtensionRequest() throws Exception {

        Path extensionDir = createTempDir();

        final ClusterService clusterService = mock(ClusterService.class);
        ClusterName clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);

        ExtensionsOrchestrator extensionsOrchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        transportService.registerRequestHandler(
            ExtensionsOrchestrator.REQUEST_EXTENSION_CLUSTER_STATE,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            (request, channel, task) -> {
                channel.sendResponse(new ClusterStateResponse(clusterName, null, false));
                assertNotNull(extensionsOrchestrator.handleExtensionRequest(request));
            }
        );

        transportService.registerRequestHandler(
            ExtensionsOrchestrator.REQUEST_EXTENSION_CLUSTER_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            (request, channel, task) -> {
                channel.sendResponse(new ClusterSettingsResponse(clusterService));
                assertNotNull(extensionsOrchestrator.handleExtensionRequest(request));
            }
        );

        transportService.registerRequestHandler(
            ExtensionsOrchestrator.REQUEST_EXTENSION_LOCAL_NODE,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            (request, channel, task) -> {
                channel.sendResponse(new LocalNodeResponse(clusterService));
                assertNotNull(extensionsOrchestrator.handleExtensionRequest(request));
            }
        );

    }

    public void testHandlerResponse() throws Exception {
        ClusterName clusterName = new ClusterName("cluster-1");
        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, null, false);
        assertEquals(clusterStateResponse.getClusterName(), clusterName);
    }

    public void testExtensionRequest() throws Exception {
        ExtensionRequest extensionRequest = new ExtensionRequest(ExtensionsOrchestrator.RequestType.REQUEST_EXTENSION_CLUSTER_STATE);
        assertEquals(extensionRequest.getRequestType(), ExtensionsOrchestrator.RequestType.REQUEST_EXTENSION_CLUSTER_STATE);
    }

    public void testOnIndexModule() throws Exception {

        Path extensionDir = createTempDir();

        List<String> extensionsYmlLines = Arrays.asList(
            "extensions:",
            "   - name: firstExtension",
            "     uniqueId: uniqueid1",
            "     hostName: 'myIndependentPluginHost1'",
            "     hostAddress: '127.0.0.0'",
            "     port: '9300'",
            "     version: '0.0.7'",
            "     description: Fake description 1",
            "     opensearchVersion: '3.0.0'",
            "     javaVersion: '14'",
            "     className: fakeClass1",
            "     customFolderName: fakeFolder1",
            "     hasNativeController: false",
            "   - name: secondExtension",
            "     uniqueId: 'uniqueid2'",
            "     hostName: 'myIndependentPluginHost2'",
            "     hostAddress: '127.0.0.1'",
            "     port: '9301'",
            "     version: '3.14.16'",
            "     description: Fake description 2",
            "     opensearchVersion: '2.0.0'",
            "     javaVersion: '17'",
            "     className: fakeClass2",
            "     customFolderName: fakeFolder2",
            "     hasNativeController: true"
        );
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        ExtensionsOrchestrator extensionsOrchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        transportService.start();
        transportService.acceptIncomingRequests();
        extensionsOrchestrator.setTransportService(transportService);

        Environment environment = TestEnvironment.newEnvironment(settings);
        AnalysisRegistry emptyAnalysisRegistry = new AnalysisRegistry(
            environment,
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        IndexModule indexModule = new IndexModule(
            indexSettings,
            emptyAnalysisRegistry,
            new InternalEngineFactory(),
            new EngineConfigFactory(indexSettings),
            Collections.emptyMap(),
            () -> true,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            Collections.emptyMap()
        );

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsOrchestrator.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "IndicesModuleRequest Failure",
                    "org.opensearch.extensions.ExtensionsOrchestrator",
                    Level.ERROR,
                    "IndicesModuleRequest failed"
                )
            );

            extensionsOrchestrator.onIndexModule(indexModule);
            mockLogAppender.assertAllExpectationsMatched();
        }
    }
}
