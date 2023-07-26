/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.identity.tokens;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.opensearch.common.Strings;
import org.opensearch.rest.RestRequest;

import java.util.Collections;
import java.util.Optional;

/**
 * Extracts tokens from RestRequests used for authentication
 */
public class RestTokenExtractor {

    private static final Logger logger = LogManager.getLogger(RestTokenExtractor.class);

    public final static String AUTH_HEADER_NAME = "Authorization";

    /**
     * Given a rest request it will extract authentication token
     *
     * If no token was found, returns null.
     */
    public static AuthToken extractToken(final RestRequest request) {

        // Extract authentication information from headers
        final Optional<String> authHeaderValue = request.getHeaders()
            .getOrDefault(AUTH_HEADER_NAME, Collections.emptyList())
            .stream()
            .findFirst();

        if (authHeaderValue.isPresent()) {
            final String authHeaderValueStr = authHeaderValue.get();

            if (authHeaderValueStr.startsWith(BasicAuthToken.TOKEN_IDENTIFIER)) {
                return new BasicAuthToken(authHeaderValueStr);
            } else {
                if (logger.isDebugEnabled()) {
                    String tokenTypeTruncated = Strings.substring(authHeaderValueStr, 0, 5);
                    logger.debug("An authentication header was detected but the token type was not supported " + tokenTypeTruncated);
                }
            }
        }

        logger.trace("No auth token could be extracted");
        return null;
    }
}
