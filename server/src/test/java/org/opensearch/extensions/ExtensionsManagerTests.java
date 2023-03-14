/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterSettingsResponse;
import org.opensearch.env.EnvironmentSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.WriteableSetting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.WriteableSetting.SettingType;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.extensions.rest.RegisterRestActionsRequest;
import org.opensearch.extensions.settings.RegisterCustomSettingsRequest;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.rest.RestController;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;
import org.opensearch.usage.UsageService;

public class ExtensionsManagerTests extends OpenSearchTestCase {

    private FeatureFlagSetter featureFlagSetter;
    private TransportService transportService;
    private RestController restController;
    private SettingsModule settingsModule;
    private ClusterService clusterService;
    private NodeClient client;
    private MockNioTransport transport;
    private Path extensionDir;
    private final ThreadPool threadPool = new TestThreadPool(ExtensionsManagerTests.class.getSimpleName());
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
        "   - name: secondExtension",
        "     uniqueId: 'uniqueid2'",
        "     hostAddress: '127.0.0.1'",
        "     port: '9301'",
        "     version: '3.14.16'",
        "     opensearchVersion: '2.0.0'",
        "     minimumCompatibleVersion: '2.0.0'",
        "     dependencies:",
        "       - uniqueId: 'uniqueid0'",
        "         version: '2.0.0'"
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
        restController = new RestController(
            emptySet(),
            null,
            new NodeClient(Settings.EMPTY, threadPool),
            new NoneCircuitBreakerService(),
            new UsageService()
        );
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

    public void testDiscover() throws Exception {
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);

        List<DiscoveryExtensionNode> expectedExtensions = new ArrayList<DiscoveryExtensionNode>();

        String expectedUniqueId = "uniqueid0";
        Version expectedVersion = Version.fromString("2.0.0");
        ExtensionDependency expectedDependency = new ExtensionDependency(expectedUniqueId, expectedVersion);

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
                List.of(expectedDependency)
            )
        );
        assertEquals(expectedExtensions.size(), extensionsManager.getExtensionIdMap().values().size());
        assertEquals(List.of(expectedDependency), expectedExtensions.get(1).getDependencies());
        for (DiscoveryExtensionNode extension : expectedExtensions) {
            DiscoveryExtensionNode initializedExtension = extensionsManager.getExtensionIdMap().get(extension.getId());
            assertEquals(extension.getName(), initializedExtension.getName());
            assertEquals(extension.getId(), initializedExtension.getId());
            assertEquals(extension.getAddress(), initializedExtension.getAddress());
            assertEquals(extension.getAttributes(), initializedExtension.getAttributes());
            assertEquals(extension.getVersion(), initializedExtension.getVersion());
            assertEquals(extension.getMinimumCompatibleVersion(), initializedExtension.getMinimumCompatibleVersion());
            assertEquals(extension.getDependencies(), initializedExtension.getDependencies());
        }
    }

    public void testNonUniqueExtensionsDiscovery() throws Exception {
        Path emptyExtensionDir = createTempDir();
        List<String> nonUniqueYmlLines = extensionsYmlLines.stream()
            .map(s -> s.replace("uniqueid2", "uniqueid1"))
            .collect(Collectors.toList());
        Files.write(emptyExtensionDir.resolve("extensions.yml"), nonUniqueYmlLines, StandardCharsets.UTF_8);

        ExtensionsManager extensionsManager = new ExtensionsManager(settings, emptyExtensionDir);

        List<DiscoveryExtensionNode> expectedExtensions = new ArrayList<DiscoveryExtensionNode>();

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
        }
        assertTrue(expectedExtensions.containsAll(emptyList()));
        assertTrue(expectedExtensions.containsAll(emptyList()));
    }

    public void testDiscoveryExtension() throws Exception {
        String expectedId = "test id";
        Version expectedVersion = Version.fromString("2.0.0");
        ExtensionDependency expectedDependency = new ExtensionDependency(expectedId, expectedVersion);

        DiscoveryExtensionNode discoveryExtensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "uniqueid1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<String, String>(),
            Version.fromString("3.0.0"),
            Version.fromString("3.0.0"),
            List.of(expectedDependency)
        );

        assertEquals(List.of(expectedDependency), discoveryExtensionNode.getDependencies());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            discoveryExtensionNode.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                discoveryExtensionNode = new DiscoveryExtensionNode(in);

                assertEquals(List.of(expectedDependency), discoveryExtensionNode.getDependencies());
            }
        }
    }

    public void testExtensionDependency() throws Exception {
        String expectedUniqueId = "Test uniqueId";
        Version expectedVersion = Version.fromString("3.0.0");

        ExtensionDependency dependency = new ExtensionDependency(expectedUniqueId, expectedVersion);

        assertEquals(expectedUniqueId, dependency.getUniqueId());
        assertEquals(expectedVersion, dependency.getVersion());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            dependency.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                dependency = new ExtensionDependency(in);
                assertEquals(expectedUniqueId, dependency.getUniqueId());
                assertEquals(expectedVersion, dependency.getVersion());
            }
        }
    }

    public void testNonAccessibleDirectory() throws Exception {
        AccessControlException e = expectThrows(

            AccessControlException.class,
            () -> new ExtensionsManager(settings, PathUtils.get(""))
        );
        assertEquals("access denied (\"java.io.FilePermission\" \"\" \"read\")", e.getMessage());
    }

    public void testNoExtensionsFile() throws Exception {
        Settings settings = Settings.builder().build();

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsManager.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "No Extensions File Present",
                    "org.opensearch.extensions.ExtensionsManager",
                    Level.WARN,
                    "Extensions.yml file is not present.  No extensions will be loaded."
                )
            );

            new ExtensionsManager(settings, extensionDir);

            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testEmptyExtensionsFile() throws Exception {
        Path emptyExtensionDir = createTempDir();

        List<String> emptyExtensionsYmlLines = Arrays.asList();
        Files.write(emptyExtensionDir.resolve("extensions.yml"), emptyExtensionsYmlLines, StandardCharsets.UTF_8);

        Settings settings = Settings.builder().build();

        expectThrows(IOException.class, () -> new ExtensionsManager(settings, emptyExtensionDir));
    }

    public void testInitialize() throws Exception {
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);

        initialize(extensionsManager);

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsManager.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Node Not Connected Exception 1",
                    "org.opensearch.extensions.ExtensionsManager",
                    Level.ERROR,
                    "[secondExtension][127.0.0.1:9301] Node not connected"
                )
            );

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Node Not Connected Exception 2",
                    "org.opensearch.extensions.ExtensionsManager",
                    Level.ERROR,
                    "[firstExtension][127.0.0.0:9300] Node not connected"
                )
            );

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "No Response From Extension",
                    "org.opensearch.extensions.ExtensionsManager",
                    Level.INFO,
                    "No response from extension to request."
                )
            );

            // Test needs to be changed to mock the connection between the local node and an extension. Assert statment is commented out for
            // now.
            // Link to issue: https://github.com/opensearch-project/OpenSearch/issues/4045
            // mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testHandleRegisterRestActionsRequest() throws Exception {
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);

        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        String uniqueIdStr = "uniqueid1";
        List<String> actionsList = List.of("GET /foo", "PUT /bar", "POST /baz");
        RegisterRestActionsRequest registerActionsRequest = new RegisterRestActionsRequest(uniqueIdStr, actionsList);
        TransportResponse response = extensionsManager.getRestActionsRequestHandler()
            .handleRegisterRestActionsRequest(registerActionsRequest);
        assertEquals(AcknowledgedResponse.class, response.getClass());
        assertTrue(((AcknowledgedResponse) response).getStatus());
    }

    public void testHandleRegisterSettingsRequest() throws Exception {
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        String uniqueIdStr = "uniqueid1";
        List<Setting<?>> settingsList = List.of(
            Setting.boolSetting("index.falseSetting", false, Property.IndexScope, Property.Dynamic),
            Setting.simpleString("fooSetting", "foo", Property.NodeScope, Property.Final)
        );
        RegisterCustomSettingsRequest registerCustomSettingsRequest = new RegisterCustomSettingsRequest(uniqueIdStr, settingsList);
        TransportResponse response = extensionsManager.getCustomSettingsRequestHandler()
            .handleRegisterCustomSettingsRequest(registerCustomSettingsRequest);
        assertEquals(AcknowledgedResponse.class, response.getClass());
        assertTrue(((AcknowledgedResponse) response).getStatus());
    }

    public void testHandleRegisterRestActionsRequestWithInvalidMethod() throws Exception {
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        String uniqueIdStr = "uniqueid1";
        List<String> actionsList = List.of("FOO /foo", "PUT /bar", "POST /baz");
        RegisterRestActionsRequest registerActionsRequest = new RegisterRestActionsRequest(uniqueIdStr, actionsList);
        expectThrows(
            IllegalArgumentException.class,
            () -> extensionsManager.getRestActionsRequestHandler().handleRegisterRestActionsRequest(registerActionsRequest)
        );
    }

    public void testHandleRegisterRestActionsRequestWithInvalidUri() throws Exception {
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);
        String uniqueIdStr = "uniqueid1";
        List<String> actionsList = List.of("GET", "PUT /bar", "POST /baz");
        RegisterRestActionsRequest registerActionsRequest = new RegisterRestActionsRequest(uniqueIdStr, actionsList);
        expectThrows(
            IllegalArgumentException.class,
            () -> extensionsManager.getRestActionsRequestHandler().handleRegisterRestActionsRequest(registerActionsRequest)
        );
    }

    public void testHandleExtensionRequest() throws Exception {
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        ExtensionRequest clusterStateRequest = new ExtensionRequest(ExtensionsManager.RequestType.REQUEST_EXTENSION_CLUSTER_STATE);
        assertEquals(ClusterStateResponse.class, extensionsManager.handleExtensionRequest(clusterStateRequest).getClass());

        ExtensionRequest clusterSettingRequest = new ExtensionRequest(ExtensionsManager.RequestType.REQUEST_EXTENSION_CLUSTER_SETTINGS);
        assertEquals(ClusterSettingsResponse.class, extensionsManager.handleExtensionRequest(clusterSettingRequest).getClass());

        ExtensionRequest environmentSettingsRequest = new ExtensionRequest(
            ExtensionsManager.RequestType.REQUEST_EXTENSION_ENVIRONMENT_SETTINGS
        );
        assertEquals(EnvironmentSettingsResponse.class, extensionsManager.handleExtensionRequest(environmentSettingsRequest).getClass());

        ExtensionRequest exceptionRequest = new ExtensionRequest(ExtensionsManager.RequestType.GET_SETTINGS);
        Exception exception = expectThrows(
            IllegalArgumentException.class,
            () -> extensionsManager.handleExtensionRequest(exceptionRequest)
        );
        assertEquals("Handler not present for the provided request", exception.getMessage());
    }

    public void testExtensionRequest() throws Exception {
        ExtensionsManager.RequestType expectedRequestType = ExtensionsManager.RequestType.REQUEST_EXTENSION_DEPENDENCY_INFORMATION;

        // Test ExtensionRequest 2 arg constructor
        String expectedUniqueId = "test uniqueid";
        ExtensionRequest extensionRequest = new ExtensionRequest(expectedRequestType, expectedUniqueId);
        assertEquals(expectedRequestType, extensionRequest.getRequestType());
        assertEquals(Optional.of(expectedUniqueId), extensionRequest.getUniqueId());

        // Test ExtensionRequest StreamInput constructor
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            extensionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                extensionRequest = new ExtensionRequest(in);
                assertEquals(expectedRequestType, extensionRequest.getRequestType());
                assertEquals(Optional.of(expectedUniqueId), extensionRequest.getUniqueId());
            }
        }

        // Test ExtensionRequest 1 arg constructor
        extensionRequest = new ExtensionRequest(expectedRequestType);
        assertEquals(expectedRequestType, extensionRequest.getRequestType());
        assertEquals(Optional.empty(), extensionRequest.getUniqueId());

        // Test ExtensionRequest StreamInput constructor
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            extensionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                extensionRequest = new ExtensionRequest(in);
                assertEquals(expectedRequestType, extensionRequest.getRequestType());
                assertEquals(Optional.empty(), extensionRequest.getUniqueId());
            }
        }
    }

    public void testExtensionDependencyResponse() throws Exception {
        String expectedUniqueId = "test uniqueid";
        List<DiscoveryExtensionNode> expectedExtensionsList = new ArrayList<DiscoveryExtensionNode>();
        Version expectedVersion = Version.fromString("2.0.0");
        ExtensionDependency expectedDependency = new ExtensionDependency(expectedUniqueId, expectedVersion);

        expectedExtensionsList.add(
            new DiscoveryExtensionNode(
                "firstExtension",
                "uniqueid1",
                new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
                new HashMap<String, String>(),
                Version.fromString("3.0.0"),
                Version.fromString("3.0.0"),
                List.of(expectedDependency)
            )
        );

        // Test ExtensionDependencyResponse arg constructor
        ExtensionDependencyResponse extensionDependencyResponse = new ExtensionDependencyResponse(expectedExtensionsList);
        assertEquals(expectedExtensionsList, extensionDependencyResponse.getExtensionDependency());

        // Test ExtensionDependencyResponse StreamInput constructor
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            extensionDependencyResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                extensionDependencyResponse = new ExtensionDependencyResponse(in);
                assertEquals(expectedExtensionsList, extensionDependencyResponse.getExtensionDependency());
            }
        }
    }

    public void testEnvironmentSettingsResponse() throws Exception {

        // Test EnvironmentSettingsResponse arg constructor
        EnvironmentSettingsResponse environmentSettingsResponse = new EnvironmentSettingsResponse(settings);
        assertEquals(settings, environmentSettingsResponse.getEnvironmentSettings());

        // Test EnvironmentSettingsResponse StreamInput constructor
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            environmentSettingsResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                environmentSettingsResponse = new EnvironmentSettingsResponse(in);
                assertEquals(settings, environmentSettingsResponse.getEnvironmentSettings());
            }
        }

    }

    public void testEnvironmentSettingsRegisteredValue() throws Exception {
        // Create setting with value false
        Setting<Boolean> boolSetting = Setting.boolSetting("boolSetting", false, Property.Dynamic);

        // Create Settings with registered bool setting with value true
        Settings environmentSettings = Settings.builder().put("boolSetting", "true").build();

        EnvironmentSettingsResponse environmentSettingsResponse = new EnvironmentSettingsResponse(environmentSettings);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            environmentSettingsResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {

                environmentSettingsResponse = new EnvironmentSettingsResponse(in);
                assertEquals(environmentSettings, environmentSettingsResponse.getEnvironmentSettings());

                // bool setting is registered in Settings object, thus the expected return value is the registered setting value
                assertEquals(true, boolSetting.get(environmentSettingsResponse.getEnvironmentSettings()));
            }
        }
    }

    public void testEnvironmentSettingsDefaultValue() throws Exception {
        // Create setting with value false
        Setting<Boolean> boolSetting = Setting.boolSetting("boolSetting", false, Property.Dynamic);

        // Create settings object without registered bool setting
        Settings environmentSettings = Settings.builder().put("testSetting", "true").build();

        EnvironmentSettingsResponse environmentSettingsResponse = new EnvironmentSettingsResponse(environmentSettings);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            environmentSettingsResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {

                environmentSettingsResponse = new EnvironmentSettingsResponse(in);
                assertEquals(environmentSettings, environmentSettingsResponse.getEnvironmentSettings());
                // bool setting is not registered in Settings object, thus the expected return value is the default setting value
                assertEquals(false, boolSetting.get(environmentSettingsResponse.getEnvironmentSettings()));
            }
        }
    }

    public void testAddSettingsUpdateConsumerRequest() throws Exception {
        Path extensionDir = createTempDir();
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        List<Setting<?>> componentSettings = List.of(
            Setting.boolSetting("falseSetting", false, Property.IndexScope, Property.NodeScope),
            Setting.simpleString("fooSetting", "foo", Property.Dynamic)
        );

        // Test AddSettingsUpdateConsumerRequest arg constructor
        AddSettingsUpdateConsumerRequest addSettingsUpdateConsumerRequest = new AddSettingsUpdateConsumerRequest(
            extensionNode,
            componentSettings
        );
        assertEquals(extensionNode, addSettingsUpdateConsumerRequest.getExtensionNode());
        assertEquals(componentSettings.size(), addSettingsUpdateConsumerRequest.getComponentSettings().size());

        List<Setting<?>> requestComponentSettings = new ArrayList<>();
        for (WriteableSetting writeableSetting : addSettingsUpdateConsumerRequest.getComponentSettings()) {
            requestComponentSettings.add(writeableSetting.getSetting());
        }
        assertTrue(requestComponentSettings.containsAll(componentSettings));
        assertTrue(componentSettings.containsAll(requestComponentSettings));

        // Test AddSettingsUpdateConsumerRequest StreamInput constructor
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            addSettingsUpdateConsumerRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                addSettingsUpdateConsumerRequest = new AddSettingsUpdateConsumerRequest(in);
                assertEquals(extensionNode, addSettingsUpdateConsumerRequest.getExtensionNode());
                assertEquals(componentSettings.size(), addSettingsUpdateConsumerRequest.getComponentSettings().size());

                requestComponentSettings = new ArrayList<>();
                for (WriteableSetting writeableSetting : addSettingsUpdateConsumerRequest.getComponentSettings()) {
                    requestComponentSettings.add(writeableSetting.getSetting());
                }
                assertTrue(requestComponentSettings.containsAll(componentSettings));
                assertTrue(componentSettings.containsAll(requestComponentSettings));
            }
        }

    }

    public void testHandleAddSettingsUpdateConsumerRequest() throws Exception {

        Path extensionDir = createTempDir();
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        List<Setting<?>> componentSettings = List.of(
            Setting.boolSetting("falseSetting", false, Property.Dynamic),
            Setting.boolSetting("trueSetting", true, Property.Dynamic)
        );

        AddSettingsUpdateConsumerRequest addSettingsUpdateConsumerRequest = new AddSettingsUpdateConsumerRequest(
            extensionNode,
            componentSettings
        );
        TransportResponse response = extensionsManager.getAddSettingsUpdateConsumerRequestHandler()
            .handleAddSettingsUpdateConsumerRequest(addSettingsUpdateConsumerRequest);
        assertEquals(AcknowledgedResponse.class, response.getClass());
        // Should fail as component settings are not registered within cluster settings
        assertEquals(false, ((AcknowledgedResponse) response).getStatus());
    }

    public void testUpdateSettingsRequest() throws Exception {
        Path extensionDir = createTempDir();
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

        Setting<?> componentSetting = Setting.boolSetting("falseSetting", false, Property.Dynamic);
        SettingType settingType = SettingType.Boolean;
        boolean data = true;

        // Test UpdateSettingRequest arg constructor
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(settingType, componentSetting, data);
        assertEquals(componentSetting, updateSettingsRequest.getComponentSetting());
        assertEquals(settingType, updateSettingsRequest.getSettingType());
        assertEquals(data, updateSettingsRequest.getData());

        // Test UpdateSettingRequest StreamInput constructor
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            updateSettingsRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                updateSettingsRequest = new UpdateSettingsRequest(in);
                assertEquals(componentSetting, updateSettingsRequest.getComponentSetting());
                assertEquals(settingType, updateSettingsRequest.getSettingType());
                assertEquals(data, updateSettingsRequest.getData());
            }
        }

    }

    public void testRegisterHandler() throws Exception {

        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);

        TransportService mockTransportService = spy(
            new TransportService(
                Settings.EMPTY,
                mock(Transport.class),
                null,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                x -> null,
                null,
                Collections.emptySet()
            )
        );
        extensionsManager.initializeServicesAndRestHandler(
            restController,
            settingsModule,
            mockTransportService,
            clusterService,
            settings,
            client
        );
        verify(mockTransportService, times(9)).registerRequestHandler(anyString(), anyString(), anyBoolean(), anyBoolean(), any(), any());

    }

    public void testOnIndexModule() throws Exception {
        Files.write(extensionDir.resolve("extensions.yml"), extensionsYmlLines, StandardCharsets.UTF_8);
        ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
        initialize(extensionsManager);

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
        expectThrows(NodeNotConnectedException.class, () -> extensionsManager.onIndexModule(indexModule));

    }

    public void testIncompatibleExtensionRegistration() throws IOException, IllegalAccessException {

        try (MockLogAppender mockLogAppender = MockLogAppender.createForLoggers(LogManager.getLogger(ExtensionsManager.class))) {

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "Could not load extension with uniqueId",
                    "org.opensearch.extensions.ExtensionsManager",
                    Level.ERROR,
                    "Could not load extension with uniqueId uniqueid1 due to OpenSearchException[Extension minimumCompatibleVersion: 3.99.0 is greater than current"
                )
            );

            List<String> incompatibleExtension = Arrays.asList(
                "extensions:",
                "   - name: firstExtension",
                "     uniqueId: uniqueid1",
                "     hostAddress: '127.0.0.0'",
                "     port: '9300'",
                "     version: '0.0.7'",
                "     opensearchVersion: '3.0.0'",
                "     minimumCompatibleVersion: '3.99.0'"
            );

            Files.write(extensionDir.resolve("extensions.yml"), incompatibleExtension, StandardCharsets.UTF_8);
            ExtensionsManager extensionsManager = new ExtensionsManager(settings, extensionDir);
            assertEquals(0, extensionsManager.getExtensionIdMap().values().size());
            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    private void initialize(ExtensionsManager extensionsManager) {
        transportService.start();
        transportService.acceptIncomingRequests();
        extensionsManager.initializeServicesAndRestHandler(
            restController,
            settingsModule,
            transportService,
            clusterService,
            settings,
            client
        );
    }
}
