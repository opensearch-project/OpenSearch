/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SdkAwarePlugin;
import org.opensearch.plugins.sdk.actions.GetSystemSettingsRequest;
import org.opensearch.plugins.sdk.actions.GetSystemSettingsResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PluginMetadataClientTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(PluginMetataClientPlugin.class);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testReadSettingViaPluginClient() throws ExecutionException, InterruptedException {
        Client client = client();

        CompletableFuture<Response> future = new CompletableFuture<>();

        client.execute(
            Action.ACTION_TYPE,
            new Request(),
            new ActionListener<Response>() {
                @Override
                public void onResponse(Response response) {
                    future.complete(response);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            }
        );

        Response response = future.get();

        assertEquals("test.case", PluginMetataClientPlugin.HELLO_WORLD_SETTING.get(response.settings));

        assertEquals("test.case", response.settings.get("hello.world"));
    }

    public void testPluginMetadataClientIsInjectable() { }
    public void testPluginMetadataClientIsAccessibleViaSdkAwarePlugin() { }

    public void testReadSystemSettingWithDefault() { }
    public void testReadSystemSettingWithoutDefault() { }
    public void testReadSystemSettingThatDoesNotExist() { }

    public void testReadIndexSettingWithDefault() { }
    public void testReadIndexSettingWithoutDefault() { }
    public void testReadIndexSettingThatDoesNotExist() { }
    public void testReadIndexSettingAcrossMultipleIndices() { }

    public void testGetIndexMappingsOnValidIndex() { }
    public void testGetIndexMappingsOnValidIndexWithWrongUUID() { }
    public void testGetIndexMappingsOnIndexThatDoesNotExist() { }
    public void testGetIndexMappingsAcrossMultipleIndices() { }

    public void testConcreteIndicesWithJustOneIndex() { }
    public void testConcreteIndicesWithWildcardExpression() { }
    public void testConcreteIndicesWithAlias() { }
    public void testConcreteIndicesWhenNoIndexExists() { }

}

class PluginMetataClientPlugin extends Plugin implements SdkAwarePlugin, ActionPlugin {

    public static final Setting<String> HELLO_WORLD_SETTING = Setting.simpleString(
        "hello.world",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> HELLO_WORLD_SETTING_WITH_DEFAULT = Setting.simpleString(
        "hello.world.has.default",
        "my.default",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private PluginMetadataClient pluginMetadataClient;

    @Override
    public void setupSdkPlugin(Dependencies dependencies) {
        this.pluginMetadataClient = dependencies.getPluginMetadataClient();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(Action.ACTION_TYPE, Action.class)
        );
    }

    @Override
    public Settings additionalSettings() {
        return Settings.builder()
            .put("hello.world", "test.case")
            .build();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(HELLO_WORLD_SETTING, HELLO_WORLD_SETTING_WITH_DEFAULT);
    }
}

class Action extends HandledTransportAction<Request, Response> {

    public static final ActionType<Response> ACTION_TYPE = new ActionType<>(
        "plugin_metadata_client_test",
        Response::new
    );

    private final PluginMetadataClient pluginMetadataClient;

    @Inject
    public Action(
        TransportService transportService,
        ActionFilters actionFilters,
        PluginMetadataClient pluginMetadataClient
    ) {
        super(ACTION_TYPE.name(), transportService, actionFilters, Request::new);
        this.pluginMetadataClient = pluginMetadataClient;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try {
            GetSystemSettingsResponse response = pluginMetadataClient.getSystemSettings(
                GetSystemSettingsRequest.builder()
                    .settings(
                        PluginMetataClientPlugin.HELLO_WORLD_SETTING.getKey(),
                        PluginMetataClientPlugin.HELLO_WORLD_SETTING_WITH_DEFAULT.getKey()
                    )
                    .build()
            ).toCompletableFuture().get();


            listener.onResponse(new Response(response.settings()));

        } catch (InterruptedException | ExecutionException e) {
            listener.onFailure(e);
        }
    }
}

class Request extends ActionRequest {

    public Request(StreamInput streamInput) {

    }

    public Request() {

    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}

class Response extends ActionResponse {

    public final Settings settings;

    public Response(Settings settings) {
        this.settings = settings;
    }

    public Response(StreamInput streamInput) throws IOException {
        throw new IllegalStateException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new IllegalStateException();
    }
}
