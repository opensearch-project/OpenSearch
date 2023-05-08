/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.tokens.AuthToken;

import java.security.Principal;

/**
 * An exception thrown when the subject is not authorized for the given permission
 *
 * @opensearch.experimental
 */
public class UnauthorizedException extends RuntimeException {
    public UnauthorizedException(final Subject subject, final String permission) {
        super("Subject " + subject.getPrincipal() + " was not authorized for permission " + permission);
    }
}