/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.extensions.ExtensionDependency;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * An action that initializes an extension
 */
public class RestInitializeExtensionAction extends BaseRestHandler {

    private final ExtensionsManager extensionsManager;
    private static final Logger logger = LogManager.getLogger(RestInitializeExtensionAction.class);

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
        String name = "";
        String uniqueId = "";
        String hostAddress = "";
        String port = "";
        String version = "";
        String openSearchVersion = "";
        String minimumCompatibleVersion = "";
        List<ExtensionDependency> dependencies = new ArrayList<>();

        if (request.hasContent()) {
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
            }
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
            // TODO create parser for additionalSettings
            null
        );
        try {
            extensionsManager.loadExtension(extension);
            extensionsManager.initialize();
        } catch (IOException e) {
            logger.error(e);
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));

        }

        logger.info("Extension has been initialized");
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("Extension has been initialized");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };

    }
}
