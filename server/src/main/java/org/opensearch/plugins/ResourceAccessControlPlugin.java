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

import java.util.Map;
import java.util.Set;

/**
 * This interface determines presence of security plugin in the cluster.
 * If yes, security plugin will be used for resource access authorization
 * User information is fetched from thread context by security plugin.
 * In clusters, where security plugin is disabled these requests will be pass-through via a No-op implementation.
 * There are 3 scope of sharing for a resource: Private, Restricted, Public. To learn more visit <a href="https://github.com/opensearch-project/security/issues/4500">...</a>
 * If security plugin is disabled, all resources will be considered public by default.
 * Refer to the sample-resource-plugin introduced <a href="https://github.com/opensearch-project/security/pull/4893">here</a> to understand the usage of this class.
 *
 * @opensearch.experimental
 */
public interface ResourceAccessControlPlugin {

    /**
     * Returns all accessible resources for current user for a given plugin index.
     * @param resourceIndex index where the resource exists
     * @param clazz class of the resource. Required to parse the resource object retrieved from resourceIndex
     * @return set of {@link ResourceSharing} items accessible by current user.
     */
    default <T> Set<T> getAccessibleResourcesForCurrentUser(String resourceIndex, Class<T> clazz) {
        return Set.of();
    }

    /**
     * Checks whether current user has permission to given resource.
     *
     * @param resourceId the resource on which access is to be checked
     * @param resourceIndex where the resource exists
     * @param scope the scope being requested
     * @return true if current user has access, false otherwise
     */
    default boolean hasPermission(String resourceId, String resourceIndex, String scope) {
        return true;
    }

    /**
     * Adds an entity to the share-with. Resource needs to be in restricted mode.
     * Creates a resource sharing record if one doesn't exist.
     * @param resourceId id of the resource to be updated
     * @param resourceIndex index where this resource is stored
     * @param shareWith an object that contains entries of entities with whom the resource should be shared with
     * @return updated resource sharing record
     */
    default ResourceSharing shareWith(String resourceId, String resourceIndex, ShareWith shareWith) {
        return null;
    }

    /**
     * Revokes given permission to a resource
     *
     * @param resourceId if of the resource to be updated
     * @param resourceIndex index where this resource is stored
     * @param revokeAccess a map that contains entries of entities whose access should be revoked
     * @param scopes Scopes to be checked for revoking access. If empty, all scopes will be checked.
     * @return the updated ResourceSharing record
     */
    default ResourceSharing revokeAccess(
        String resourceId,
        String resourceIndex,
        Map<EntityType, Set<String>> revokeAccess,
        Set<String> scopes
    ) {
        return null;
    }

    /**
     * Deletes an entry from .resource_sharing index
     * @param resourceId if of the resource to be updated
     * @param resourceIndex index where this resource is stored
     * @return true if resource record was deleted, false otherwise
     */
    default boolean deleteResourceSharingRecord(String resourceId, String resourceIndex) {
        return false;
    }

    /**
     * TODO check if this method is needed
     * Deletes all entries from .resource_sharing index where current user is the creator of the resource
     * @return true if resource record was deleted, false otherwise
     */
    default boolean deleteAllResourceSharingRecordsForCurrentUser() {
        return false;
    }

    // TODO: Check whether methods for bulk updates are required
}
