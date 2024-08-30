/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.accesscontrol.resources.EntityType;
import org.opensearch.accesscontrol.resources.ResourceSharing;

import java.util.List;
import java.util.Map;

/**
 * This plugin class defines a no-op implementation of Resource Plugin.
 *
 * @opensearch.experimental
 */
public class NoOpResourceAccessControlPlugin implements ResourceAccessControlPlugin {

    /**
     *
     * @return an empty map of resource names accessible by this user.
     */
    @Override
    public Map<String, List<String>> listAccessibleResources() {
        return Map.of();
    }

    /**
     * Returns an empty list since security plugin is not defined.
     * This method alone doesn't determine permissions.
     *
     * @return empty list
     */
    @Override
    public List<String> listAccessibleResourcesForPlugin(String systemIndexName) {
        // returns an empty list since security plugin is disabled
        return List.of();
    }

    /**
     * Returns true since no authorization is required.
     *
     * @param resourceId the resource on which access is to be checked
     * @param systemIndexName where the resource exists
     * @return true
     */
    @Override
    public boolean hasPermission(String resourceId, String systemIndexName) {
        return true;
    }

    /**
     * Adds an entity to the share-with. Resource needs to be in restricted mode.
     *
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @param revokeAccess a map that contains entries of entities with whom this resource should be shared with
     * @return null since security plugin is disabled in the cluster
     */
    @Override
    public ResourceSharing shareWith(String resourceId, String systemIndexName, Map<EntityType, List<String>> revokeAccess) {
        return null;
    }

    /**
     * Revokes access to the resource
     *
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @param revokeAccess a map that contains entries of entities whose access should be revoked
     * @return null since security plugin is disabled in the cluster
     */
    @Override
    public ResourceSharing revokeAccess(String resourceId, String systemIndexName, Map<EntityType, List<String>> revokeAccess) {
        return null;
    }

    /**
     * Delete a resource sharing record
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @return false since security plugin is disabled
     */
    @Override
    public boolean deleteResourceSharingRecord(String resourceId, String systemIndexName) {
        return false;
    }

}
