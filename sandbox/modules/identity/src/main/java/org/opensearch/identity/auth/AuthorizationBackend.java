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
 * OpenSearch Security custom authorization backends need to implement this interface.
 *
 * Authorization backends populate a prior authenticated {@link User} with backend roles who's the user is a member of.
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
public interface AuthorizationBackend {

    /**
     * The type (name) of the authorizer. Only for logging.
     * @return the type
     */
    String getType();

    /**
     * Populate a {@link User} with backend roles. This method will not be called for cached users.
     *
     * Add them by calling either {@code user.addRole()} or {@code user.addRoles()}
     *
     * @param user The authenticated user to populate with backend roles, never null
     * @param credentials Credentials to authenticate to the authorization backend, maybe null.
     * <em>This parameter is for future usage, currently always empty credentials are passed!</em>
     * @throws OpenSearchSecurityException in case when the authorization backend cannot be reached
     * or the {@code credentials} are insufficient to authenticate to the authorization backend.
     */
    void fillRoles(User user, AuthCredentials credentials) throws OpenSearchSecurityException;

}


