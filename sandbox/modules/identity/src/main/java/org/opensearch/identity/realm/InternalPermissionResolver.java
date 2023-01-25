/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.realm;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

public class InternalPermissionResolver implements PermissionResolver {
    @Override
    public Permission resolvePermission(String permissionString) {
        return null;
    }
}
