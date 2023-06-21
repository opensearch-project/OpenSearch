/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.ExtensionDependency;
import org.opensearch.extensions.ExtensionScopedSettings;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.transport.ConnectTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
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
        return List.of(new Route(POST, "/_extensions/initialize"));
    }

    public RestInitializeExtensionAction(ExtensionsManager extensionsManager) {
        this.extensionsManager = extensionsManager;
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

        try (XContentParser parser = request.contentParser()) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String currentFieldName = parser.currentName();
                parser.nextToken();
                if ("name".equals(currentFieldName)) {
                    name = parser.text();
                } else if ("uniqueId".equals(currentFieldName)) {
                    uniqueId = parser.text();
                } else if ("hostAddress".equals(currentFieldName)) {
                    hostAddress = parser.text();
                } else if ("port".equals(currentFieldName)) {
                    port = parser.text();
                } else if ("version".equals(currentFieldName)) {
                    version = parser.text();
                } else if ("opensearchVersion".equals(currentFieldName)) {
                    openSearchVersion = parser.text();
                } else if ("minimumCompatibleVersion".equals(currentFieldName)) {
                    minimumCompatibleVersion = parser.text();
                } else if ("dependencies".equals(currentFieldName)) {
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        dependencies.add(ExtensionDependency.parse(parser));
                    }
                }
            }
        } catch (IOException e) {
            throw new IOException("Missing attribute", e);
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
            // TODO add this to the API (https://github.com/opensearch-project/OpenSearch/issues/8032)
            new ExtensionScopedSettings(Collections.emptySet())
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
