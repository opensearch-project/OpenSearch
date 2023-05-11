/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionModule;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.env.Environment;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.Subject;
import org.opensearch.identity.noop.NoopSubject;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;
import org.opensearch.usage.UsageService;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class ExtensionsManagerWithIdentityTests extends OpenSearchTestCase {

    private FeatureFlagSetter featureFlagSetter;
    private TransportService transportService;
    private ActionModule actionModule;
    private RestController restController;
    private SettingsModule settingsModule;
    private ClusterService clusterService;
    private IdentityService identityService;

    private Setting customSetting = Setting.simpleString("custom_extension_setting", "none", Property.ExtensionScope);
    private NodeClient client;
    private MockNioTransport transport;
    private Path extensionDir;
    private final ThreadPool threadPool = new TestThreadPool(ExtensionsManagerWithIdentityTests.class.getSimpleName());
    private final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();
    private final List<String> extensionsYmlLines = Arrays.asList(
        "extensions:",
        "   - name: firstExtension",
        "     uniqueId: uniqueid1",
        "     hostAddress: '127.0.0.0'",
        "     port: '9300'",
        "     version: '0.0.7'",
        "     opensearchVersion: '3.0.0'",
        "     minimumCompatibleVersion: '3.0.0'",
        "     custom_extension_setting: 'custom_setting'",
        "   - name: secondExtension",
        "     uniqueId: 'uniqueid2'",
        "     hostAddress: '127.0.0.1'",
        "     port: '9301'",
        "     version: '3.14.16'",
        "     opensearchVersion: '2.0.0'",
        "     minimumCompatibleVersion: '2.0.0'"
    );

    private DiscoveryExtensionNode extensionNode;

    @Before
    public void setup() throws Exception {
        featureFlagSetter = FeatureFlagSetter.set(FeatureFlags.EXTENSIONS);
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        transport = new MockNioTransport(
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
        actionModule = mock(ActionModule.class);
        IdentityPlugin identityPlugin = new IdentityPlugin() {
            @Override
            public Subject getSubject() {
                return new NoopSubject();
            }

            @Override
            public List<Setting<?>> getExtensionSettings() {
                List<Setting<?>> settings = new ArrayList<Setting<?>>();
                settings.add(customSetting);
                return settings;
            }
        };
        identityService = new IdentityService(Settings.EMPTY, List.of(identityPlugin));
        restController = new RestController(
            emptySet(),
            null,
            new NodeClient(Settings.EMPTY, threadPool),
            new NoneCircuitBreakerService(),
            new UsageService(),
            identityService
        );
        when(actionModule.getRestController()).thenReturn(restController);
        settingsModule = new SettingsModule(Settings.EMPTY, emptyList(), emptyList(), emptySet());
        clusterService = createClusterService(threadPool);

        extensionDir = createTempDir();

        extensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "uniqueid1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<String, String>(),
            Version.fromString("3.0.0"),
            Version.fromString("3.0.0"),
            Collections.emptyList()
        );
        client = new NoOpNodeClient(this.getTestName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        featureFlagSetter.close();
    }

    public void testAdditionalExtensionSettings() throws Exception {
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        ExtensionsManager extensionsManager = new ExtensionsManager(extensionDir, identityService);

        List<DiscoveryExtensionNode> expectedExtensions = new ArrayList<DiscoveryExtensionNode>();

        String expectedUniqueId = "uniqueid0";

        expectedExtensions.add(
            new DiscoveryExtensionNode(
                "firstExtension",
                "uniqueid1",
                new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
                new HashMap<String, String>(),
                Version.fromString("3.0.0"),
                Version.fromString("3.0.0"),
                Collections.emptyList()
            )
        );

        expectedExtensions.add(
            new DiscoveryExtensionNode(
                "secondExtension",
                "uniqueid2",
                new TransportAddress(InetAddress.getByName("127.0.0.1"), 9301),
                new HashMap<String, String>(),
                Version.fromString("2.0.0"),
                Version.fromString("2.0.0"),
                List.of()
            )
        );
        assertEquals(expectedExtensions.size(), extensionsManager.getExtensionIdMap().values().size());
        for (DiscoveryExtensionNode extension : expectedExtensions) {
            DiscoveryExtensionNode initializedExtension = extensionsManager.getExtensionIdMap().get(extension.getId());
            assertEquals(extension.getName(), initializedExtension.getName());
            assertEquals(extension.getId(), initializedExtension.getId());
            assertEquals(extension.getAddress(), initializedExtension.getAddress());
            assertEquals(extension.getAttributes(), initializedExtension.getAttributes());
            assertEquals(extension.getVersion(), initializedExtension.getVersion());
            assertEquals(extension.getMinimumCompatibleVersion(), initializedExtension.getMinimumCompatibleVersion());
            assertEquals(extension.getDependencies(), initializedExtension.getDependencies());
            assertTrue(extensionsManager.lookupExtensionSettingsById(extension.getId()).isPresent());
            if ("firstExtension".equals(extension.getName())) {
                assertEquals("custom_setting", extensionsManager.lookupExtensionSettingsById(extension.getId()).get().getAdditionalSettings().get(customSetting));
            } else if ("secondExtension".equals(extension.getName())) {
                assertEquals("none", extensionsManager.lookupExtensionSettingsById(extension.getId()).get().getAdditionalSettings().get(customSetting));
            }
        }
    }
}
