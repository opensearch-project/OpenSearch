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

    private String actionName;

    private List<String> indexPatterns;

    public OpenSearchPermission(String permission) {
        Objects.requireNonNull(permission);
        String[] permissionParts = permission.split("\\|");
        // turns an action like indices:data/read/search into indices:data:read:search to leverage shiro's WildcardPermission
        this.actionName = permissionParts[0].replace("/", ":");
        if (permissionParts.length > 1) {
            this.indexPatterns = Arrays.stream(permissionParts[1].split(",")).map(String::trim).collect(Collectors.toList());
        }
    }

    @Override
    public boolean implies(Permission p) {
        if (!(p instanceof OpenSearchPermission)) {
            return false;
        }
        OpenSearchPermission requestedPermission = (OpenSearchPermission) p;

        WildcardPermission wp = new WildcardPermission(this.actionName);
        WildcardPermission wp2 = new WildcardPermission(requestedPermission.actionName);
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

        boolean allRequestedPatternsMatchGranted = requestedPermission.indexPatterns.stream()
            .allMatch(requstedIp -> this.indexPatterns.stream().anyMatch(ip -> Glob.globMatch(ip, requstedIp)));

        return allRequestedPatternsMatchGranted;
    }
}
