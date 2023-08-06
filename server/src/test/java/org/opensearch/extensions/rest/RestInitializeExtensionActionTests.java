/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class RestInitializeExtensionActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private MockNioTransport transport;
    private final ThreadPool threadPool = new TestThreadPool(RestInitializeExtensionActionTests.class.getSimpleName());

    @Before
    public void setup() throws Exception {
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

    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRestInitializeExtensionActionResponse() throws Exception {
        ExtensionsManager extensionsManager = mock(ExtensionsManager.class);
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(extensionsManager);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"opensearchVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(channel.capturedResponse().status(), RestStatus.ACCEPTED);
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));
    }

    public void testRestInitializeExtensionActionFailure() throws Exception {
        ExtensionsManager extensionsManager = new ExtensionsManager(Set.of());
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(extensionsManager);

        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"opensearchVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(1, channel.errors().get());
        assertTrue(
            channel.capturedResponse().content().utf8ToString().contains("Required field [extension uniqueId] is missing in the request")
        );
    }

    public void testRestInitializeExtensionActionResponseWithAdditionalSettings() throws Exception {
        Setting boolSetting = Setting.boolSetting("boolSetting", false, Setting.Property.ExtensionScope);
        Setting stringSetting = Setting.simpleString("stringSetting", "default", Setting.Property.ExtensionScope);
        Setting intSetting = Setting.intSetting("intSetting", 0, Setting.Property.ExtensionScope);
        Setting listSetting = Setting.listSetting(
            "listSetting",
            List.of("first", "second", "third"),
            Function.identity(),
            Setting.Property.ExtensionScope
        );
        ExtensionsManager extensionsManager = new ExtensionsManager(Set.of(boolSetting, stringSetting, intSetting, listSetting));
        ExtensionsManager spy = spy(extensionsManager);

        // optionally, you can stub out some methods:
        when(spy.getAdditionalSettings()).thenCallRealMethod();
        Mockito.doCallRealMethod().when(spy).loadExtension(any(ExtensionsSettings.Extension.class));
        Mockito.doNothing().when(spy).initialize();
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(spy);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"opensearchVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\",\"boolSetting\":true,\"stringSetting\":\"customSetting\",\"intSetting\":5,\"listSetting\":[\"one\",\"two\",\"three\"]}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(channel.capturedResponse().status(), RestStatus.ACCEPTED);
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));

        Optional<ExtensionsSettings.Extension> extension = spy.lookupExtensionSettingsById("ad-extension");
        assertTrue(extension.isPresent());
        assertEquals(true, extension.get().getAdditionalSettings().get(boolSetting));
        assertEquals("customSetting", extension.get().getAdditionalSettings().get(stringSetting));
        assertEquals(5, extension.get().getAdditionalSettings().get(intSetting));

        List<String> listSettingValue = (List<String>) extension.get().getAdditionalSettings().get(listSetting);
        assertTrue(listSettingValue.contains("one"));
        assertTrue(listSettingValue.contains("two"));
        assertTrue(listSettingValue.contains("three"));
    }

    public void testRestInitializeExtensionActionResponseWithAdditionalSettingsUsingDefault() throws Exception {
        Setting boolSetting = Setting.boolSetting("boolSetting", false, Setting.Property.ExtensionScope);
        Setting stringSetting = Setting.simpleString("stringSetting", "default", Setting.Property.ExtensionScope);
        Setting intSetting = Setting.intSetting("intSetting", 0, Setting.Property.ExtensionScope);
        Setting listSetting = Setting.listSetting(
            "listSetting",
            List.of("first", "second", "third"),
            Function.identity(),
            Setting.Property.ExtensionScope
        );
        ExtensionsManager extensionsManager = new ExtensionsManager(Set.of(boolSetting, stringSetting, intSetting, listSetting));
        ExtensionsManager spy = spy(extensionsManager);

        // optionally, you can stub out some methods:
        when(spy.getAdditionalSettings()).thenCallRealMethod();
        Mockito.doCallRealMethod().when(spy).loadExtension(any(ExtensionsSettings.Extension.class));
        Mockito.doNothing().when(spy).initialize();
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(spy);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"opensearchVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(channel.capturedResponse().status(), RestStatus.ACCEPTED);
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));

        Optional<ExtensionsSettings.Extension> extension = spy.lookupExtensionSettingsById("ad-extension");
        assertTrue(extension.isPresent());
        assertEquals(false, extension.get().getAdditionalSettings().get(boolSetting));
        assertEquals("default", extension.get().getAdditionalSettings().get(stringSetting));
        assertEquals(0, extension.get().getAdditionalSettings().get(intSetting));

        List<String> listSettingValue = (List<String>) extension.get().getAdditionalSettings().get(listSetting);
        assertTrue(listSettingValue.contains("first"));
        assertTrue(listSettingValue.contains("second"));
        assertTrue(listSettingValue.contains("third"));
    }

}
