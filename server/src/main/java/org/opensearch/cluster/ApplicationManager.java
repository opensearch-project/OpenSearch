/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.Application;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ServiceAccountManager;
import org.opensearch.identity.noop.NoopServiceAccountManager;
import org.opensearch.plugins.PluginsService;

/**
 * The ApplicationManager class handles the processing and resolution of multiple types of applications. Using the class, OpenSearch can
 * continue to resolve requests even when specific application types are disabled. For example, the ExtensionManager can be Noop in which case
 * the ApplicationManager is able to resolve requests for other application types still
 *
 * @opensearch.experimental
 */
public class ApplicationManager {

    AtomicReference<ExtensionsManager> extensionManager;
    AtomicReference<PluginsService> pluginManager;
    AtomicReference<ServiceAccountManager> serviceAccountManager;
    public static ApplicationManager instance; // Required for access in static contexts
    private List<Application> registeredApplications = new ArrayList<>(); // A list of all application subjects

    public ApplicationManager() {
        instance = this;
        extensionManager = new AtomicReference<>();
        pluginManager = new AtomicReference<>();
        serviceAccountManager = new AtomicReference<>();
    }

    public void register(ExtensionsManager manager) {
        if (this.extensionManager == null) {
            this.extensionManager = new AtomicReference<>(manager);
        } else {
            this.extensionManager.set(manager);
        }
    }

    public void register(PluginsService manager) {
        if (this.pluginManager == null) {
            this.pluginManager = new AtomicReference<>(manager);
        } else {
            this.pluginManager.set(manager);
        }
    }

    public void register(ServiceAccountManager manager) {
        if (this.serviceAccountManager == null) {
            this.serviceAccountManager = new AtomicReference<>(manager);
        } else {
            this.serviceAccountManager.set(manager);
        }
    }

    public static ApplicationManager getInstance() {
        if (instance == null) {
            new ApplicationManager();
        }
        return instance;
    }

    /**
     * Checks whether there is an application associated with the given principal or not
     * @param principal The principal for the application you are trying to find
     * @return Whether the application exists (TRUE) or not (FALSE)
     */
    public boolean associatedApplicationExists(Principal principal) {
        return (this.extensionManager.get().getExtensionIdMap().containsKey(principal.getName()));
    }

    /**
     * Allows for checking the ExtensionManager being used by the ApplicationManager in the case that there are multiple application providers
     * @return The ExtensionManager being queried by the ApplicationManager
     */
    public ExtensionsManager getExtensionManager() {
        return extensionManager.get();
    }

    /**
     * Allows for accessing the ServiceAccountManager
     * @return The ExtensionManager being queried by the ApplicationManager
     */
    public ServiceAccountManager getServiceAccountManager() {
        if (serviceAccountManager.get() == null) {
            this.register(new NoopServiceAccountManager());
        }
        return serviceAccountManager.get();
    }
}
