/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Set;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.NoopExtensionsManager;
import org.opensearch.identity.ApplicationSubject;
import org.opensearch.identity.scopes.Scope;

/**
 * A NoopApplicationManager that is used to bypass all checks during Node tests.
 */
public class NoopApplicationManager extends ApplicationManager {

    public NoopApplicationManager(ExtensionsManager extensionsManager) {
        super(extensionsManager);
    }

    public NoopApplicationManager() throws IOException {
        super(new NoopExtensionsManager());
    }

    @Override
    public boolean isAllowed(ApplicationSubject wrapped, final List<Scope> scopes) {
        return true;
    }

    @Override
    public boolean associatedApplicationExists(Principal principal) {
        return true;
    }

    @Override
    public Set<Scope> getScopes(Principal principal) {
        return Set.of();
    }
}
