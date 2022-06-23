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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
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
import org.opensearch.plugins.PluginTestUtil;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class ExtensionsOrchestratorTests extends OpenSearchTestCase {

    public void testExtensionsDiscovery() throws Exception {
        Path extensionDir = createTempDir();

        List<String> extensionsYmlLines = Arrays.asList(
            "extensions:",
            "   - name: firstExtension",
            "     uniqueId: uniqueid1",
            "     hostName: 'myIndependentPluginHost1'",
            "     hostAddress: '127.0.0.0'",
            "     port: '9300'",
            "     version: '3.0.0'",
            "   - name: secondExtension",
            "     uniqueId: 'uniqueid2'",
            "     hostName: 'myIndependentPluginHost2'",
            "     hostAddress: '127.0.0.1'",
            "     port: '9301'",
            "     version: '2.0.0'"
        );
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        Path pluginDir1 = extensionDir.resolve("firstExtension");
        Path pluginDir2 = extensionDir.resolve("secondExtension");

        PluginTestUtil.writePluginProperties(
            pluginDir1,
            "description",
            "fake desc",
            "name",
            "firstExtension",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakeExtensions"
        );

        PluginTestUtil.writePluginProperties(
            pluginDir2,
            "description",
            "fake desc",
            "name",
            "secondExtension",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakeExtensions"
        );

        Settings settings = Settings.builder().build();
        ExtensionsOrchestrator orchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        Set<DiscoveryExtension> expectedExtensionsSet = new HashSet<DiscoveryExtension>();

        expectedExtensionsSet.add(
            new DiscoveryExtension(
                "firstExtension",
                "id",
                "uniqueid1",
                "myIndependentPluginHost1",
                "127.0.0.0",
                new TransportAddress(TransportAddress.META_ADDRESS, 9300),
                new HashMap<String, String>(),
                Version.fromString("3.0.0"),
                PluginInfo.readFromProperties(pluginDir1)
            )
        );

        expectedExtensionsSet.add(
            new DiscoveryExtension(
                "secondExtension",
                "id",
                "uniqueid2",
                "myIndependentPluginHost2",
                "127.0.0.1",
                new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                new HashMap<String, String>(),
                Version.fromString("2.0.0"),
                PluginInfo.readFromProperties(pluginDir2)
            )
        );
        assertEquals(expectedExtensionsSet, orchestrator.extensionsSet);
    }

    public void testNonAccessibleDirectory() throws Exception {
        Settings settings = Settings.builder().put("node.name", "my-node").build();
        ;
        AccessControlException e = expectThrows(
            AccessControlException.class,
            () -> new ExtensionsOrchestrator(settings, PathUtils.get(""))
        );
        assertEquals("access denied (\"java.io.FilePermission\" \"\" \"read\")", e.getMessage());
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
            "     version: '3.0.0'",
            "   - name: secondExtension",
            "     uniqueId: 'uniqueid2'",
            "     hostName: 'myIndependentPluginHost2'",
            "     hostAddress: '127.0.0.1'",
            "     port: '9301'",
            "     version: '2.0.0'"
        );
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        Path pluginDir1 = extensionDir.resolve("fake-extension-1");
        Path pluginDir2 = extensionDir.resolve("fake-extension-2");

        PluginTestUtil.writePluginProperties(
            pluginDir1,
            "description",
            "fake desc",
            "name",
            "firstExtension",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakeExtensions"
        );

        PluginTestUtil.writePluginProperties(
            pluginDir2,
            "description",
            "fake desc",
            "name",
            "secondExtension",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakeExtensions"
        );

        Settings settings = Settings.builder().put("cluster.name", "test").build();
        ExtensionsOrchestrator orchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        ThreadPool threadPool = new TestThreadPool(ExtensionsOrchestratorTests.class.getSimpleName());
        MockNioTransport transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()
        );

        TransportService transportService = new MockTransportService(
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
        transportService.start();
        transportService.acceptIncomingRequests();
        orchestrator.setTransportService(transportService);

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsOrchestrator.class))) {
        
            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Transport Connect Exception 2",
                    "org.opensearch.extensions.ExtensionsOrchestrator",
                    Level.ERROR,
                    "ConnectTransportException[[secondExtension][0.0.0.0:9301] connect_exception]; nested: ConnectException[Connection refused];"
                )
            );

            orchestrator.extensionsInitialize();

            transportService.close();
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
            mockLogAppender.assertAllExpectationsMatched();
        }
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
            "     version: '3.0.0'",
            "   - name: secondExtension",
            "     uniqueId: 'uniqueid2'",
            "     hostName: 'myIndependentPluginHost2'",
            "     hostAddress: '127.0.0.1'",
            "     port: '9301'",
            "     version: '2.0.0'"
        );
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        Path pluginDir1 = extensionDir.resolve("fake-extension-1");
        Path pluginDir2 = extensionDir.resolve("fake-extension-2");

        PluginTestUtil.writePluginProperties(
            pluginDir1,
            "description",
            "fake desc",
            "name",
            "firstExtension",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakeExtensions"
        );

        PluginTestUtil.writePluginProperties(
            pluginDir2,
            "description",
            "fake desc",
            "name",
            "secondExtension",
            "version",
            "1.0",
            "opensearch.version",
            Version.CURRENT.toString(),
            "java.version",
            System.getProperty("java.specification.version"),
            "classname",
            "FakeExtensions"
        );

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        ExtensionsOrchestrator orchestrator = new ExtensionsOrchestrator(settings, extensionDir);

        ThreadPool threadPool = new TestThreadPool(ExtensionsOrchestratorTests.class.getSimpleName());
        MockNioTransport transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService()
        );

        TransportService transportService = new MockTransportService(
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
        transportService.start();
        transportService.acceptIncomingRequests();
        orchestrator.setTransportService(transportService);

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

            orchestrator.onIndexModule(indexModule);

            transportService.close();
            ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
            mockLogAppender.assertAllExpectationsMatched();
        }
    }
}
