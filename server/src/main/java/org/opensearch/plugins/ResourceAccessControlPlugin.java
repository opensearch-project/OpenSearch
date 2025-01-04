/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.accesscontrol.resources.RecipientType;
import org.opensearch.accesscontrol.resources.Resource;
import org.opensearch.accesscontrol.resources.ResourceSharing;
import org.opensearch.accesscontrol.resources.ShareWith;

import java.util.Map;
import java.util.Set;

/**
 * This plugin allows to control access to resources. It is used by the ResourcePlugins to check whether a user has access to a resource defined by that plugin.
 * It also defines java APIs to list, share or revoke resources with other users.
 * User information will be fetched from the ThreadContext.
 *
 * @opensearch.experimental
 */
public interface ResourceAccessControlPlugin {

    /**
     * Returns all accessible resources for current user for a given plugin index.
     * @param resourceIndex index where the resource exists
     * @param clazz class of the resource. Required to parse the resource object retrieved from resourceIndex. Must be a type of {@link Resource}
     * @return set of {@link ResourceSharing} items accessible by current user.
     */
    default <T extends Resource> Set<T> getAccessibleResourcesForCurrentUser(String resourceIndex, Class<T> clazz) {
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
     * Adds an entity to the share-with.
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
     * Revokes given scope permission to a resource
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
        Map<RecipientType, Set<String>> revokeAccess,
        Set<String> scopes
    ) {
        return null;
    }

    /**
     * Deletes a resource sharing record
     *
     * @param resourceId if of the resource to delete the resource sharing record
     * @param resourceIndex index where this resource is stored
     * @return true if resource record was deleted, false otherwise
     */
    default boolean deleteResourceSharingRecord(String resourceId, String resourceIndex) {
        return false;
    }

    /**
     * Deletes all resource sharing records for current user
     * @return true if resource records were deleted, false otherwise
     */
    default boolean deleteAllResourceSharingRecordsForCurrentUser() {
        return false;
    }

}
