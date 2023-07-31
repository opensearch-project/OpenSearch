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
import org.opensearch.Application;
import org.opensearch.common.collect.Tuple;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.ExtensionsSettings;
import org.opensearch.identity.ServiceAccountManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;

/**
 * The ApplicationManager class handles the processing and resolution of multiple types of applications. Using the class, OpenSearch can
 * continue to resolve requests even when specific application types are disabled. For example, the ExtensionManager can be Noop in which case
 * the ApplicationManager is able to resolve requests for other application types still
 *
 * @opensearch.experimental
 */
public final class ApplicationManager {

    private final ExtensionsManager extensionManager;
    private final PluginsService pluginManager;
    private final ServiceAccountManager serviceAccountManager;
    private List<Application> registeredApplications = new ArrayList<>(); // A list of all application subjects

    public ApplicationManager(
        ExtensionsManager extensionsManager,
        PluginsService pluginsService,
        ServiceAccountManager serviceAccountManager
    ) {
        extensionManager = extensionsManager;
        pluginManager = pluginsService;
        this.serviceAccountManager = serviceAccountManager;
    }

    /**
     * Checks whether there is an application associated with the given principal or not
     * @param principal The principal for the application you are trying to find
     * @return Whether the application exists (TRUE) or not (FALSE)
     */
    public boolean associatedApplicationExists(Principal principal) {
        return (this.extensionManager.getExtensionIdMap().containsKey(principal.getName()));
    }

    /**
     * Allows for checking the ExtensionManager being used by the ApplicationManager in the case that there are multiple application providers
     * @return The ExtensionManager being queried by the ApplicationManager
     */
    public ExtensionsManager getExtensionManager() {
        return extensionManager;
    }

    /**
     * Allows for accessing the ServiceAccountManager
     * @return The ExtensionManager being queried by the ApplicationManager
     */
    public ServiceAccountManager getServiceAccountManager() {
        return serviceAccountManager;
    }

    /**
     * Register all plugins and modules loaded by the PluginsService and add them to the application store
     */
    public void registerPluginsAndModules() {
        List<Tuple<PluginInfo, Plugin>> pluginsLoaded = pluginManager.getPlugins();
        // Register service accounts for all loaded plugins
        for (Tuple<PluginInfo, Plugin> pluginTuple : pluginsLoaded) {
            PluginInfo pluginInfo = pluginTuple.v1();
            Plugin plugin = pluginTuple.v2();

            serviceAccountManager.getServiceAccount(pluginInfo);
        }
    }

    /**
     * Registers an Extension with the ApplicationManager
     * @param extension The extension to be registered
     */
    public void registerExtension(ExtensionsSettings.Extension extension) {
		registeredApplications.add(extension);
        serviceAccountManager.getServiceAccount(extension);
	}
}
