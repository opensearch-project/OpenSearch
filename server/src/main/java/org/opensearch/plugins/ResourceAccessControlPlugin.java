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
import org.opensearch.accesscontrol.resources.ShareWith;

import java.util.List;
import java.util.Map;

/**
 * This interface determines presence of security plugin in the cluster.
 * If yes, security plugin will be used for resource access authorization
 * User information is fetched from thread context by security plugin.
 * In clusters, where security plugin is disabled these requests will be pass-through via a No-op implementation.
 * There are 3 scope of sharing for a resource: Private, Restricted, Public. To learn more visit <a href="https://github.com/opensearch-project/security/issues/4500">...</a>
 * If security plugin is disabled, all resources will be considered public by default.
 * TODO: add documentation around "how to use"
 *
 * @opensearch.experimental
 */
public interface ResourceAccessControlPlugin {
    /**
     * Returns all accessible resources for current user.
     * @return a map of resources accessible by user separated by system index names
     */
    Map<String, List<String>> listAccessibleResources();

    /**
     * Returns all accessible resources for current user for a given system .
     *
     * @return list of {@link ResourceSharing} items accessible by current user.
     */
    List<String> listAccessibleResourcesForPlugin(String systemIndex);

    /**
     * Checks whether current user has permission to given resource.
     *
     * @param resourceId the resource on which access is to be checked
     * @param systemIndexName where the resource exists
     * @param scope the scope being requested
     * @return true if current user has access, false otherwise
     */
    boolean hasPermission(String resourceId, String systemIndexName, String scope);

    /**
     * Adds an entity to the share-with. Resource needs to be in restricted mode.
     * Creates a resource sharing record if one doesn't exist.
     * @param resourceId id of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @param shareWith an object that contains entries of entities with whom the resource should be shared with
     * @return updated resource sharing record
     */
    ResourceSharing shareWith(String resourceId, String systemIndexName, ShareWith shareWith);

    /**
     * Revokes given permission to a resource
     *
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @param revokeAccess a map that contains entries of entities whose access should be revoked
     * @return the updated ResourceSharing record
     */
    ResourceSharing revokeAccess(String resourceId, String systemIndexName, Map<EntityType, List<String>> revokeAccess);

    /**
     * Deletes an entry from .resource_sharing index
     * @param resourceId if of the resource to be updated
     * @param systemIndexName index where this resource is defined
     * @return true if resource record was deleted, false otherwise
     */
    boolean deleteResourceSharingRecord(String resourceId, String systemIndexName);

    /**
     * TODO check if this method is needed
     * Deletes all entries from .resource_sharing index where current user is the creator of the resource
     * @return true if resource record was deleted, false otherwise
     */
    boolean deleteAllResourceSharingRecordsForCurrentUser();

    // TODO: Check whether methods for bulk updates are required
}
