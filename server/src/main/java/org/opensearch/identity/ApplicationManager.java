/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Optional;
import java.util.Set;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.scopes.Scope;

/**
 * The ApplicationManager class handles the processing and resolution of multiple types of applications. Using the class, OpenSearch can
 * continue to resolve requests even when specific application types are disabled. For example, the ExtensionManager can be Noop in which case
 * the ApplicationManager is able to resolve requests for other application types still
 *
 * @opensearch.experimental
 */
public class ApplicationManager {

    private final AtomicReference<ExtensionsManager> extensionManager = new AtomicReference<ExtensionsManager>();

    public void register(final ExtensionsManager manager) {
        // Only allow the extension manager to be registered the first time
        extensionManager.compareAndSet(null, manager);
    }

    public Optional<Set<Scope>> getScopes(final Principal applicationPrincipal) {
        return findExtension(applicationPrincipal);
    }

    private Optional<Set<Scope>> findExtension(final Principal applicationPrincipal) {
        if (extensionManager.get().getExtensionIdMap().containsKey(applicationPrincipal.getName())) {
            return Optional.of(extensionManager.get().getExtensionIdMap().get(applicationPrincipal.getName()).getScopes());
        }
        return Optional.empty();
    }
}
