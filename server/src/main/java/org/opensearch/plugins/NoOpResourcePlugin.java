/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.accesscontrol.resources.Resource;
import org.opensearch.accesscontrol.resources.ShareWith;

import java.util.List;
import java.util.Map;

/**
 * This plugin class defines a no-op implementation of Resource Plugin.
 *
 * @opensearch.experimental
 */
public class NoOpResourcePlugin implements ResourcePlugin {

    /**
     * Returns an empty list since security plugin is not defined.
     * This method alone doesn't determine permissions.
     *
     * @return empty list
     */
    @Override
    public List<Resource> listAccessibleResources() {
        // returns an empty list since security plugin is disabled
        return List.of();
    }

    /**
     * Returns true since no authorization is required.
     *
     * @param resource the resource on which access is to be checked
     * @return true
     */
    public boolean hasPermission(Resource resource) {
        return true;
    }

    /**
     * Adds an entity to the share-with. Resource needs to be in restricted mode.
     *
     * @param type One of the {@link ShareWith} types
     * @param entities List of names with whom to share this resource with
     * @return a message whether sharing was successful.
     */
    public String shareWith(ShareWith type, List<String> entities) {

        return "Unable to share as security plugin is disabled in the cluster.";
    }

    /**
     * Revokes access to the resource
     *
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @param revokeAccess a map that contains entries of entities whose access should be revoked
     * @return false since no resource-sharing information is required as security plugin is disabled
     */
    @Override
    public boolean revoke(String resourceId, String systemIndexName, Map<ShareWith, List<String>> revokeAccess) {
        return false;
    }

    /**
     * Delete a resource sharing record
     * @param resource The resource to be removed from the index
     * @return false since security plugin is disabled
     */
    @Override
    public boolean deleteResourceSharingRecord(Resource resource) {
        return false;
    }

}
