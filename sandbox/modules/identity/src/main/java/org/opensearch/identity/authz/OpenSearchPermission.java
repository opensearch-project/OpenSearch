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
import java.util.Objects;
import java.util.stream.Collectors;

public class OpenSearchPermission implements Permission {

    private List<String> indexPatterns;
    public final String PERMISSION_DELIMITER = "\\.";

    public final String ACTION_DELIMITER = "/";

    public String permissionString;
    public String[] permissionSegments;
    public String resource;

    public String action;
    public String permissionType;

    public OpenSearchPermission(String permission) {

        this.permissionString = permission;
        try {
            this.permissionSegments = permissionString.split(PERMISSION_DELIMITER);
            this.permissionType = permissionSegments[0];
            this.action = permissionSegments[1];
        } catch (IndexOutOfBoundsException ex) {
            throw new PermissionFactory.InvalidPermissionException(
                "All permissions must contain a permission type and" + " action delimited by a \".\"."
            );
        }
        if (this.permissionSegments.length == 3) {
            this.resource = permissionSegments[2];
        }
    }


    @Override
    public boolean implies(Permission p) {
        if (!(p instanceof OpenSearchPermission)) {
            return false;
        }
        OpenSearchPermission requestedPermission = (OpenSearchPermission) p;

        WildcardPermission wp = new WildcardPermission(this.action);
        WildcardPermission wp2 = new WildcardPermission(requestedPermission.action);
        if (!wp.implies(wp2)) {
            return false;
        }
        if (this.indexPatterns == null || this.indexPatterns.isEmpty()) {
            // TODO Find a better way to do this, this means only the permission does not require a resource
            // and is permitted by name alone
            return true;
        }
        if (requestedPermission.indexPatterns == null || requestedPermission.indexPatterns.isEmpty()) {
            return false;
        }

        // Uncomment the following lines if index name -> concrete index resolution is required
        // IndexNameExpressionResolver iner = IndexNameExpressionResolverHolder.getInstance();
        // ClusterState cs = IdentityPlugin.GuiceHolder.getClusterService().state();
        // Set<String> concretePermissionIndexNames = iner.resolveExpressions(cs, this.indexPatterns.toArray(new String[0]));
        // Set<String> concreteRequestedIndexNames = iner.resolveExpressions(cs, requestedPermission.indexPatterns.toArray(new String[0]));

        // TODO Switch this comments to tests to better document and assert different scenarios
        // Example
        // User granted index patterns: logs-*,my-index*
        // Request permission on: logs-2023-01-27
        //
        // For all requested index patterns (ip):
        // ip must match at least one pattern that the user has been granted access to

        boolean allRequestedPatternsMatchGranted = requestedPermission.indexPatterns.stream()
            .allMatch(requstedIp -> this.indexPatterns.stream().anyMatch(ip -> Glob.globMatch(ip, requstedIp)));

        return allRequestedPatternsMatchGranted;
    }
}
