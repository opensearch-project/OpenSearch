/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.authn.HttpHeaderToken;
import org.opensearch.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.node.Node.INTERNAL_REALM;

/**
 * Adds a wrapper to all rest requests to add authentication mechanism
 *
 * Reference: <a href="https://github.com/opensearch-project/security/blob/main/src/main/java/org/opensearch/security/filter/SecurityRestFilter.java">SecurityRestFilter.java</a>
 */
public class SecurityRestFilter {

    private static final Logger logger = LogManager.getLogger(SecurityRestFilter.class);
    public static final String LEGACY_OPENDISTRO_PREFIX = "_opendistro/_security";
    public static final String PLUGINS_PREFIX = "_plugins/_security";
    private static final String REGEX_PATH_PREFIX = "/(" + LEGACY_OPENDISTRO_PREFIX + "|" + PLUGINS_PREFIX + ")/" + "(.*)";
    private static final Pattern PATTERN_PATH_PREFIX = Pattern.compile(REGEX_PATH_PREFIX);
    private static final String HEALTH_SUFFIX = "health";
    private static final String WHO_AM_I_SUFFIX = "whoami";

    public SecurityRestFilter() {
        super();
        // TODO modify this constructor and add all relevant configs
    }

    /**
     * This function adds a wrapper for all incoming rest requests
     * @param original the original rest handler that will be used to proceed with request once it is authenticated
     * @return the RestHandler wrapper to original Rest Handler
     */
    public RestHandler wrap(RestHandler original) {
        return (request, channel, client) -> {
            if (checkAndAuthenticateRequest(request, channel, client)) {
                // TODO: Do allowlisting and isAdmin check
                original.handleRequest(request, channel, client);
            }
        };
    }

    /**
     * Checks if incoming REST request is correctly formed
     * and authenticates the subject of the request
     *
     * @param request the incoming rest request to be authenticated
     * @param channel the channel to be used to proceed with request handling
     * @param client the node the that handles the request
     * @return true if authentication was successful, false otherwise
     * @throws Exception generic exception rethrown in case authenticate method throws an exception
     */
    private boolean checkAndAuthenticateRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        Matcher matcher = PATTERN_PATH_PREFIX.matcher(request.path());
        final String suffix = matcher.matches() ? matcher.group(2) : null;

        // following two requests are authenticated by default??
        if (HEALTH_SUFFIX.equals(suffix) && WHO_AM_I_SUFFIX.equals(suffix)) {
            return true;
        }

        return authenticate(request, channel, client);
    }

    /**
     * Authenticates the subject of the incoming REST request based on the auth header
     * @param request the request whose subject is to be authenticated
     * @param channel the channel to send the response on
     * @param client the client to be used
     * @return true if authentication was successful, false otherwise
     * @throws IOException when an exception is raised writing response to channel
     */
    private boolean authenticate(RestRequest request, RestChannel channel, NodeClient client) throws IOException {

        final Optional<String> authHeader = request.getHeaders()
            .getOrDefault(HttpHeaderToken.HEADER_NAME, Collections.emptyList())
            .stream()
            .findFirst();

        if (authHeader.isPresent()) {
            try {
                HttpHeaderToken token = new HttpHeaderToken(authHeader.get());
                INTERNAL_REALM.authenticateWithToken(token);
                return true;
            } catch (final Exception e) {
                final BytesRestResponse bytesRestResponse = BytesRestResponse.createSimpleErrorResponse(
                    channel,
                    RestStatus.UNAUTHORIZED,
                    e.getMessage()
                );
                channel.sendResponse(bytesRestResponse);
                return false;
            }
        }
        logger.info("Authentication finally failed due to missing auth header");
        // TODO: Is this response correct
        final BytesRestResponse bytesRestResponse = BytesRestResponse.createSimpleErrorResponse(
            channel,
            RestStatus.BAD_REQUEST,
            "Authentication failed: Missing Credentials"
        );
        channel.sendResponse(bytesRestResponse);
        return false;
    }

}
