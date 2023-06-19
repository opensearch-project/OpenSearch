/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.opensearch.action.ActionScope;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.identity.ApplicationScope;
import org.opensearch.plugins.PluginsService;

/**
 * This class defines an ApplicationService which acts as a registry for all plugin and extension managers.
 * Using this class, you can combine all the different types of applications supported by OpenSearch. You can also add additional application
 * providers without needing to modify the existing IdentityService.
 */
public class ApplicationService {

    private ArrayList<ExtensionsManager> extensionsManagerList = new ArrayList<>();
    private ArrayList<PluginsService> pluginsServiceList = new ArrayList<>();
    private HashMap<String, Set<String>> principalScopeMap = new HashMap<String, Set<String>>();

    public ApplicationService() {};

    public ApplicationService(ArrayList<ExtensionsManager> extensionsManagers, ArrayList<PluginsService> pluginsServices) {
        this.extensionsManagerList = extensionsManagers;
        this.pluginsServiceList = pluginsServices;
        updatePrincipalScopeMap();
    }

    public void setExtensionsManagerList(ArrayList<ExtensionsManager> extensionsManagers) {
        this.extensionsManagerList = extensionsManagers;
        updatePrincipalScopeMap();
    }

    public void setPluginsServiceList(ArrayList<PluginsService> pluginsServices) {
        this.pluginsServiceList = pluginsServices;
        updatePrincipalScopeMap();
    }

    public ArrayList<ExtensionsManager> getExtensionsManagerList() {
        return extensionsManagerList;
    }


    public ArrayList<PluginsService> getPluginsServiceList() {
        return pluginsServiceList;
    }

    public HashMap<String, Set<String>> getPrincipalScopeMap() {
        return principalScopeMap;
    }

    public Set<String> getScopes(String principal) {
        return principalScopeMap.get(principal);
    }

    public Set<String> getActionScopes(String principal) {

    }

    // This is expensive so should try to avoid doing it. Split into subroutines and then trigger individually?
    private void updatePrincipalScopeMap() {

        extensionsManagerList.forEach(extensionsManager ->
            extensionsManager.getExtensionIdMap().forEach((key, value) -> principalScopeMap.put(key, value.getScopes()))
        );

        pluginsServiceList.forEach(pluginsService -> pluginsService.getPluginsAndModules().getPluginInfos().forEach((key) -> principalScopeMap.put(key.getName(), Set.of(
            ActionScope.ALL.asPermissionString(), ApplicationScope.SuperUserAccess.asPermissionString())))); // Plugins have all scopes

        pluginsServiceList.forEach(pluginsService -> pluginsService.getPluginsAndModules().getModuleInfos().forEach((key) -> principalScopeMap.put(key.getName(), Set.of(
            ActionScope.ALL.asPermissionString(), ApplicationScope.SuperUserAccess.asPermissionString())))); // Modules have all scopes
    }
}
