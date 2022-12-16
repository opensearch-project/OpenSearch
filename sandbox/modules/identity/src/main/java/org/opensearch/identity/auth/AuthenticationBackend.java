/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.auth;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.authn.User;

/**
 * OpenSearch Security custom authentication backends need to implement this interface.
 *
 * Authentication backends verify {@link AuthCredentials} and, if successfully verified, return a {@link User}.
 *
 * Implementation classes must provide a public constructor
 *
 * {@code public MyHTTPAuthenticator(org.opensearch.common.settings.Settings settings, java.nio.file.Path configPath)}
 *
 * The constructor should not throw any exception in case of an initialization problem.
 * Instead catch all exceptions and log a appropriate error message. A logger can be instantiated like:
 *
 * {@code private final Logger log = LogManager.getLogger(this.getClass());}
 *
 */
public interface AuthenticationBackend {

    /**
     * The type (name) of the authenticator. Only for logging.
     * @return the type
     */
    String getType();

    /**
     * Validate credentials and return an authenticated user (or throw an OpenSearchSecurityException)
     *
     * Results of this method are normally cached so that we not need to query the backend for every authentication attempt.
     *
     * @param credentials The credentials to be validated, never null
     * @return the authenticated User, never null
     * @throws OpenSearchSecurityException in case an authentication failure
     * (when credentials are incorrect, the user does not exist or the backend is not reachable)
     */
    User authenticate(AuthCredentials credentials) throws OpenSearchSecurityException;

    /**
     *
     * Lookup for a specific user in the authentication backend
     *
     * @param user The user for which the authentication backend should be queried. If the authentication backend supports
     * user attributes in combination with impersonation the attributes needs to be added to user by calling {@code user.addAttributes()}
     * @return true if the user exists in the authentication backend, false otherwise. Before return call {@code user.addAttributes()} as explained above.
     */
    boolean exists(User user);

}

