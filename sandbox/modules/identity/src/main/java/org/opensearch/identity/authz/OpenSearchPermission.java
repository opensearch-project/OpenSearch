/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.opensearch.common.Glob;

import java.util.Arrays;
import java.util.List;

public class OpenSearchPermission implements Permission {

    private List<String> resourcePatterns;
    private final String PERMISSION_DELIMITER = "\\.";

    public final String RESOURCE_DELIMITER = ",";

    private String permissionString;
    private String[] permissionSegments;

    private String action;
    private String permissionType;

    public OpenSearchPermission(String permission) {

        this.permissionString = permission;
        try {
            this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
            this.permissionType = permissionSegments[0];
            this.action = permissionSegments[1];
        } catch (IndexOutOfBoundsException ex) {
            throw new PermissionFactory.InvalidPermissionException(
                "All permissions must contain a permission type and action delimited by a " + PERMISSION_DELIMITER + "."
            );
        }
        // Handle two legacy index permissions that are really cluster permissions
        if (this.action.startsWith("admin/template/") || this.action.startsWith("admin/index_template/")) {
            this.resourcePatterns = Arrays.asList("*");
        }
        if (this.permissionSegments.length == 3) {
            String resourceString = permissionSegments[2];
            this.resourcePatterns = Arrays.asList(resourceString.split(RESOURCE_DELIMITER));

        }
    }

    public String getPermissionType() {
        return this.permissionType;
    }

    public String getAction() {
        return this.action;
    }

    public List<String> getResource() {
        return this.resourcePatterns;
    }

    public String getPermissionString() {
        return this.permissionString;
    }

    /**
     * Compare the current permission's permission type to another permission's permission type
     */
    public boolean permissionTypesMatch(OpenSearchPermission permission) {

        if (this.permissionType.equalsIgnoreCase(permission.permissionType)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean implies(Permission p) {
        if (!(p instanceof OpenSearchPermission)) {
            return false;
        }
        OpenSearchPermission requestedPermission = (OpenSearchPermission) p;

        // Check if permission types match
        if (!permissionTypesMatch(requestedPermission)) {
            return false;
        }

        // Check if permission actions match
        WildcardPermission wp = new WildcardPermission(this.action);
        WildcardPermission wp2 = new WildcardPermission(requestedPermission.action);
        if (!wp.implies(wp2)) {
            return false;
        }

        // Check if resource pattern is empty and resolve
        if (this.resourcePatterns == null || this.resourcePatterns.isEmpty()) {

            // If the matching permission type requires a resource pattern
            if (PermissionFactory.QUALIFIED_PERMISSION_TYPES.matchingType(this.permissionType).isResourcePatternRequired()) {

                return false;
            } else {
                return true;
            }
        }

        // IndexNameExpressionResolver iner = IndexNameExpressionResolverHolder.getInstance();
        // ClusterState cs = IdentityPlugin.GuiceHolder.getClusterService().state();
        // Set<String> concretePermissionIndexNames = iner.resolveExpressions(cs, this.resourcePatterns.toArray(new String[0]));
        // Set<String> concreteRequestedIndexNames = iner.resolveExpressions(cs, requestedPermission.resourcePatterns.toArray(new
        // String[0]));

        // TODO Switch this comments to tests to better document and assert different scenarios
        // Example
        // User granted index patterns: logs-*,my-index*
        // Request permission on: logs-2023-01-27
        //
        // For all requested index patterns (ip):
        // ip must match at least one pattern that the user has been granted access to

        boolean allRequestedPatternsMatchGranted = requestedPermission.resourcePatterns.stream()
            .allMatch(requstedIp -> this.resourcePatterns.stream().anyMatch(ip -> Glob.globMatch(ip, requstedIp)));

        return allRequestedPatternsMatchGranted;
    }

    public boolean equals(OpenSearchPermission secondPermission) {
        if (this.getPermissionString() == secondPermission.getPermissionString()) {
            return true;
        }
        return false;
    }

}
