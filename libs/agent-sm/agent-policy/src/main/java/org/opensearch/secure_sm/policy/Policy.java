/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.util.Enumeration;

/**
 * OpenSearch policy abstraction used by the agent-based security checks.
 *
 * <p>This class intentionally does not extend or install {@code java.security.Policy}. It provides only the small permission-checking
 * contract OpenSearch needs while the JDK removes legacy Security Manager APIs.
 */
public class Policy {

    /**
     * Shared read-only empty permission collection for implementations that do not support permission enumeration.
     */
    public static final PermissionCollection EMPTY_PERMISSION_COLLECTION = emptyPermissionCollection();

    private static final Policy DEFAULT_POLICY = new Policy();

    /**
     * Returns the default OpenSearch policy.
     *
     * <p>The default policy has no external policy file. It only reflects static permissions already present on the queried protection
     * domain.
     *
     * @return default policy instance
     */
    public static Policy getPolicy() {
        return DEFAULT_POLICY;
    }

    /**
     * Returns permissions associated with a protection domain.
     *
     * <p>The base implementation copies static permissions already attached to the domain. File-backed policies override this to add
     * permissions loaded from policy files.
     *
     * @param domain protection domain to inspect
     * @return mutable permission collection containing the domain's static permissions
     */
    public PermissionCollection getPermissions(ProtectionDomain domain) {
        Permissions permissions = new Permissions();
        if (domain == null) {
            return permissions;
        }
        copyPermissions(domain.getPermissions(), permissions);
        return permissions;
    }

    /**
     * Checks whether the policy grants a permission to a protection domain.
     *
     * @param domain protection domain to inspect
     * @param permission permission being checked
     * @return {@code true} when the policy grants the permission
     */
    public boolean implies(ProtectionDomain domain, Permission permission) {
        if (domain == null || permission == null) {
            return false;
        }
        return getPermissions(domain).implies(permission);
    }

    /**
     * Refreshes policy state. Implementations backed by external resources may override this.
     */
    public void refresh() {}

    /**
     * Copies permissions from one collection to another.
     *
     * @param source source collection, may be {@code null}
     * @param target target collection
     */
    protected static void copyPermissions(PermissionCollection source, PermissionCollection target) {
        if (source == null) {
            return;
        }
        synchronized (source) {
            Enumeration<Permission> permissions = source.elements();
            while (permissions.hasMoreElements()) {
                target.add(permissions.nextElement());
            }
        }
    }

    private static PermissionCollection emptyPermissionCollection() {
        Permissions permissions = new Permissions();
        permissions.setReadOnly();
        return permissions;
    }
}
