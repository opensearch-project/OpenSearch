/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.ServiceAccountManager;
import org.opensearch.identity.UserProvider;
import org.opensearch.plugins.IdentityPlugin;
import org.opensearch.identity.Subject;

/**
 * Implementation of identity plugin that does not enforce authentication or authorization
 *
 * This class and related classes in this package will not return nulls or fail access checks
 *
 * @opensearch.internal
 */
public class NoopIdentityPlugin implements IdentityPlugin {

    /**
     * Get the current subject
     * @return Must never return null
     */
    @Override
    public Subject getSubject() {
        return new NoopSubject();
    }

    @Override
    public ServiceAccountManager getServiceAccountManager() {
        return new NoopServiceAccountManager();
    }

    @Override
    public UserProvider getUserProvider() {
        return null;
    }
}
