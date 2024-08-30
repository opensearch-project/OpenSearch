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
 * This plugin class defines usage mechanisms for plugins to interact with resources.
 * User information is fetched from thread context by security plugin.
 * In clusters, where security plugin is disabled these requests will be pass-through via a No-op implementation.
 * There are 3 scope of sharing for a resource: Private, Restricted, Public. To learn more visit <a href="https://github.com/opensearch-project/security/issues/4500">...</a>
 * If security plugin is disabled, all resources will be considered public by default.
 * TODO: add documentation around "how to use"
 *
 *
 *
 * @opensearch.experimental
 */
public interface ResourcePlugin {

    /**
     * Returns all accessible resources for current user.
     *
     * @return list of {@link Resource} items accessible by current user.
     */
    List<Resource> listAccessibleResources();

    /**
     * Checks whether current user has permission to given resource.
     *
     *
     * @param resource the resource on which access is to be checked
     * @return true if current user has access, false otherwise
     */
    boolean hasPermission(Resource resource);

    /**
     * Adds an entity to the share-with. Resource needs to be in restricted mode.
     * @param type One of the {@link ShareWith} types
     * @param entities List of names with whom to share this resource with
     * @return a message whether sharing was successful.
     */
    String shareWith(ShareWith type, List<String> entities);

    /**
     * Revokes given permission to a resource
     *
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @param revokeAccess a map that contains entries of entities whose access should be revoked
     * @return true if revoke was successful, false if there was a failure
     */
    boolean revoke(String resourceId, String systemIndexName, Map<ShareWith, List<String>> revokeAccess);

    /**
     * Deletes an entry from .resource_sharing index
     * @param resource The resource to be removed from the index
     * @return true if resource record was deleted, false otherwise
     */
    boolean deleteResourceSharingRecord(Resource resource);

    // TODO: Check whether methods for bulk updates are required
}
