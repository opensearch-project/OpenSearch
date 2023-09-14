/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.Version;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.extensions.ExtensionDependency;
import org.opensearch.extensions.ExtensionScopedSettings;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * An action that initializes an extension
 */
public class RestInitializeExtensionAction extends BaseRestHandler {

    private final ExtensionsManager extensionsManager;

    @Override
    public String getName() {
        return ExtensionsManager.REQUEST_EXTENSION_ACTION_NAME;
    }

    @Override
    public List<Route> routes() {
        return List.of(new NamedRoute.Builder().method(POST).path("/_extensions/initialize").uniqueName("extensions:initialize").build());
    }

    public RestInitializeExtensionAction(ExtensionsManager extensionsManager) {
        this.extensionsManager = extensionsManager;
    }

    private static Map<String, Object> unflattenMap(Map<String, Object> flatMap) {
        Map<String, Object> unflattenedMap = new HashMap<>();

        for (Map.Entry<String, Object> entry : flatMap.entrySet()) {
            String[] keys = entry.getKey().split("\\.");
            putNested(unflattenedMap, keys, entry.getValue());
        }

        return unflattenedMap;
    }

    private static void putNested(Map<String, Object> map, String[] keys, Object value) {
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];

            if (i == keys.length - 1) {
                map.put(key, value);
            } else if (keys[i + 1].matches("\\d+")) {
                int index = Integer.parseInt(keys[++i]);

                List<Map<String, Object>> list;
                if (map.containsKey(key)) {
                    list = (List<Map<String, Object>>) map.get(key);
                } else {
                    list = new ArrayList<>();
                    map.put(key, list);
                }

                while (list.size() <= index) {
                    list.add(new HashMap<>());
                }

                map = list.get(index);
            } else {
                Map<String, Object> nestedMap;
                if (map.containsKey(key)) {
                    nestedMap = (Map<String, Object>) map.get(key);
                } else {
                    nestedMap = new HashMap<>();
                    map.put(key, nestedMap);
                }

                map = nestedMap;
            }
        }
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = null;
        String uniqueId = null;
        String hostAddress = null;
        String port = null;
        String version = null;
        String openSearchVersion = null;
        String minimumCompatibleVersion = null;
        List<ExtensionDependency> dependencies = new ArrayList<>();
        Set<String> additionalSettingsKeys = extensionsManager.getAdditionalSettings()
            .stream()
            .map(s -> s.getKey())
            .collect(Collectors.toSet());

        Tuple<? extends MediaType, Map<String, Object>> unreadExtensionTuple = XContentHelper.convertToMap(
            request.content(),
            false,
            request.getMediaType().xContent().mediaType()
        );
        Map<String, Object> extensionMap = unreadExtensionTuple.v2();

        ExtensionScopedSettings extAdditionalSettings = new ExtensionScopedSettings(extensionsManager.getAdditionalSettings());

        try {
            // checking to see whether any required fields are missing from extension initialization request or not
            String[] requiredFields = {
                "name",
                "uniqueId",
                "hostAddress",
                "port",
                "version",
                "opensearchVersion",
                "minimumCompatibleVersion" };
            List<String> missingFields = Arrays.stream(requiredFields)
                .filter(field -> !extensionMap.containsKey(field))
                .collect(Collectors.toList());
            if (!missingFields.isEmpty()) {
                throw new IOException("Extension is missing these required fields : " + missingFields);
            }

            // Parse extension dependencies
            List<ExtensionDependency> extensionDependencyList = new ArrayList<ExtensionDependency>();
            if (extensionMap.get("dependencies") != null) {
                List<HashMap<String, ?>> extensionDependencies = new ArrayList<>(
                    (Collection<HashMap<String, ?>>) extensionMap.get("dependencies")
                );
                for (HashMap<String, ?> dependency : extensionDependencies) {
                    if (Strings.isNullOrEmpty((String) dependency.get("uniqueId"))) {
                        throw new IOException("Required field [uniqueId] is missing in the request for the dependent extension");
                    } else if (dependency.get("version") == null) {
                        throw new IOException("Required field [version] is missing in the request for the dependent extension");
                    }
                    extensionDependencyList.add(
                        new ExtensionDependency(
                            dependency.get("uniqueId").toString(),
                            Version.fromString(dependency.get("version").toString())
                        )
                    );
                }
            }

            Map<String, Object> additionalSettingsMap = extensionMap.entrySet()
                .stream()
                    .filter(kv -> additionalSettingsKeys.stream().anyMatch(k -> {
                        if (k.endsWith(".")) {
                            return kv.getKey().startsWith(k);
                        } else {
                            return kv.getKey().equals(k);
                        }
                    })).collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()));

            Settings.Builder output = Settings.builder();
            Map<String, Object> unflattenedMap = unflattenMap(additionalSettingsMap);
            output.loadFromMap(unflattenedMap);
            extAdditionalSettings.applySettings(output.build());

            // Create extension read from initialization request
            name = extensionMap.get("name").toString();
            uniqueId = extensionMap.get("uniqueId").toString();
            hostAddress = extensionMap.get("hostAddress").toString();
            port = extensionMap.get("port").toString();
            version = extensionMap.get("version").toString();
            openSearchVersion = extensionMap.get("opensearchVersion").toString();
            minimumCompatibleVersion = extensionMap.get("minimumCompatibleVersion").toString();
            dependencies = extensionDependencyList;
        } catch (IOException e) {
            logger.warn("loading extension has been failed because of exception : " + e.getMessage());
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
        }

        Extension extension = new Extension(
            name,
            uniqueId,
            hostAddress,
            port,
            version,
            openSearchVersion,
            minimumCompatibleVersion,
            dependencies,
            extAdditionalSettings
        );
        try {
            extensionsManager.loadExtension(extension);
            extensionsManager.initialize();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, "No response from extension to request.")
                );
            } else if (cause instanceof ConnectTransportException || cause instanceof RuntimeException) {
                return channel -> channel.sendResponse(
                    new BytesRestResponse(RestStatus.REQUEST_TIMEOUT, "Connection failed with the extension.")
                );
            }
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
        } catch (Exception e) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));

        }

        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("success", "A request to initialize an extension has been sent.");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.ACCEPTED, builder));
            }
        };
    }
}
