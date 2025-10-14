/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import org.opensearch.secure_sm.AccessController;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.util.Enumeration;
import java.util.WeakHashMap;

/**
 * A {@code Policy} object is responsible for determining whether code executing
 * in the Java runtime environment has permission to perform a
 * security-sensitive operation.
 *
 * <p> There is only one {@code Policy} object installed in the runtime at any
 * given time.  A {@code Policy} object can be installed by calling the
 * {@code setPolicy} method.  The installed {@code Policy} object can be
 * obtained by calling the {@code getPolicy} method.
 *
 * <p> If no {@code Policy} object has been installed in the runtime, a call to
 * {@code getPolicy} installs an instance of the default {@code Policy}
 * implementation (a default subclass implementation of this abstract class).
 * The default {@code Policy} implementation can be changed by setting the value
 * of the {@code policy.provider} security property to the fully qualified
 * name of the desired {@code Policy} subclass implementation. The system
 * class loader is used to load this class.
 *
 * <p> Application code can directly subclass {@code Policy} to provide a custom
 * implementation.  In addition, an instance of a {@code Policy} object can be
 * constructed by invoking one of the {@code getInstance} factory methods
 * with a standard type.  The default policy type is "JavaPolicy".
 *
 * <p> Once a {@code Policy} instance has been installed (either by default,
 * or by calling {@code setPolicy}), the Java runtime invokes its
 * {@code implies} method when it needs to
 * determine whether executing code (encapsulated in a ProtectionDomain)
 * can perform SecurityManager-protected operations.  How a {@code Policy}
 * object retrieves its policy data is up to the {@code Policy} implementation
 * itself. The policy data may be stored, for example, in a flat ASCII file,
 * in a serialized binary file of the {@code Policy} class, or in a database.
 *
 * <p> The {@code refresh} method causes the policy object to
 * refresh/reload its data.  This operation is implementation-dependent.
 * For example, if the policy object stores its data in configuration files,
 * calling {@code refresh} will cause it to re-read the configuration
 * policy files.  If a refresh operation is not supported, this method does
 * nothing.  Note that refreshed policy may not have an effect on classes
 * in a particular ProtectionDomain. This is dependent on the policy
 * provider's implementation of the {@code implies}
 * method and its PermissionCollection caching strategy.
 *
 * @author Roland Schemers
 * @author Gary Ellison
 * @see java.security.ProtectionDomain
 * @see java.security.Permission
 * @see java.security.Security security properties
 */

public abstract class Policy {

    /**
     * Constructor for subclasses to call.
     */
    public Policy() {}

    /**
     * A read-only empty PermissionCollection instance.
     */
    public static final PermissionCollection EMPTY_PERMISSION_COLLECTION = new EmptyPermissionCollection();

    // Information about the system-wide policy.
    private static class PolicyInfo {
        // the system-wide policy
        final Policy policy;
        // a flag indicating if the system-wide policy has been initialized
        final boolean initialized;

        PolicyInfo(Policy policy, boolean initialized) {
            this.policy = policy;
            this.initialized = initialized;
        }
    }

    // PolicyInfo is volatile since we apply DCL during initialization.
    // For correctness, care must be taken to read the field only once and only
    // write to it after any other initialization action has taken place.
    private static volatile Policy.PolicyInfo policyInfo = new Policy.PolicyInfo(null, false);

    // Cache mapping ProtectionDomain.Key to PermissionCollection
    private WeakHashMap<Integer, PermissionCollection> pdMapping;

    /**
     * Returns the installed {@code Policy} object.
     *
     * @return the installed Policy.
     *
     * @see #setPolicy(Policy)
     */
    public static Policy getPolicy() {
        Policy.PolicyInfo pi = policyInfo;

        if (pi.initialized == false || pi.policy == null) {
            try {
                Path emptyPolicyFile = Files.createTempFile("empty", ".tmp");
                final Policy emptyPolicy = new PolicyFile(emptyPolicyFile.toUri().toURL());
                pi = new PolicyInfo(emptyPolicy, true);
                // IOUtils.rm(emptyPolicyFile);
            } catch (Exception e) {
                // ignore
            }
        }
        return pi.policy;
    }

    /**
     * Sets the system-wide {@code Policy} object.
     *
     * @param p the new system {@code Policy} object.
     *
     * @see #getPolicy()
     *
     */
    public static void setPolicy(Policy p) {
        if (p != null) {
            initPolicy(p);
        }
        synchronized (Policy.class) {
            policyInfo = new Policy.PolicyInfo(p, p != null);
        }
    }

    /**
     * Initialize superclass state such that a legacy provider can
     * handle queries for itself.
     */
    private static void initPolicy(final Policy p) {
        /*
         * A policy provider not on the bootclasspath could trigger
         * security checks fulfilling a call to either Policy.implies
         * or Policy.getPermissions. If this does occur the provider
         * must be able to answer for it's own ProtectionDomain
         * without triggering additional security checks, otherwise
         * the policy implementation will end up in an infinite
         * recursion.
         *
         * To mitigate this, the provider can collect it's own
         * ProtectionDomain and associate a PermissionCollection while
         * it is being installed. The currently installed policy
         * provider (if there is one) will handle calls to
         * Policy.implies or Policy.getPermissions during this
         * process.
         *
         * This Policy superclass caches away the ProtectionDomain and
         * statically binds permissions so that legacy Policy
         * implementations will continue to function.
         */

        ProtectionDomain policyDomain = AccessController.doPrivileged(() -> p.getClass().getProtectionDomain());

        /*
         * Collect the permissions granted to this protection domain
         * so that the provider can be security checked while processing
         * calls to Policy.implies or Policy.getPermissions.
         */
        PermissionCollection policyPerms = null;
        synchronized (p) {
            if (p.pdMapping == null) {
                p.pdMapping = new WeakHashMap<>();
            }
        }

        if (policyDomain.getCodeSource() != null) {
            Policy pol = policyInfo.policy;
            if (pol != null) {
                policyPerms = pol.getPermissions(policyDomain);
            }

            synchronized (p.pdMapping) {
                // cache of pd to permissions
                p.pdMapping.put(policyDomain.getCodeSource().hashCode(), policyPerms);
            }
        }
    }

    /**
     * Return a PermissionCollection object containing the set of
     * permissions granted to the specified CodeSource.
     *
     * <p> Applications are discouraged from calling this method
     * since this operation may not be supported by all policy implementations.
     * Applications should solely rely on the {@code implies} method
     * to perform policy checks.  If an application absolutely must call
     * a getPermissions method, it should call
     * {@code getPermissions(ProtectionDomain)}.
     *
     * <p> The default implementation of this method returns
     * Policy.EMPTY_PERMISSION_COLLECTION.  This method can be
     * overridden if the policy implementation can return a set of
     * permissions granted to a CodeSource.
     *
     * @param codesource the CodeSource to which the returned
     *          PermissionCollection has been granted.
     *
     * @return a set of permissions granted to the specified CodeSource.
     *          If this operation is supported, the returned
     *          set of permissions must be a new mutable instance
     *          and it must support heterogeneous Permission types.
     *          If this operation is not supported,
     *          Policy.EMPTY_PERMISSION_COLLECTION is returned.
     */
    public PermissionCollection getPermissions(CodeSource codesource) {
        return Policy.EMPTY_PERMISSION_COLLECTION;
    }

    /**
     * Return a PermissionCollection object containing the set of
     * permissions granted to the specified ProtectionDomain.
     *
     * <p> Applications are discouraged from calling this method
     * since this operation may not be supported by all policy implementations.
     * Applications should rely on the {@code implies} method
     * to perform policy checks.
     *
     * <p> The default implementation of this method first retrieves
     * the permissions returned via {@code getPermissions(CodeSource)}
     * (the CodeSource is taken from the specified ProtectionDomain),
     * as well as the permissions located inside the specified ProtectionDomain.
     * All of these permissions are then combined and returned in a new
     * PermissionCollection object.  If {@code getPermissions(CodeSource)}
     * returns Policy.EMPTY_PERMISSION_COLLECTION, then this method
     * returns the permissions contained inside the specified ProtectionDomain
     * in a new PermissionCollection object.
     *
     * <p> This method can be overridden if the policy implementation
     * supports returning a set of permissions granted to a ProtectionDomain.
     *
     * @param domain the ProtectionDomain to which the returned
     *          PermissionCollection has been granted.
     *
     * @return a set of permissions granted to the specified ProtectionDomain.
     *          If this operation is supported, the returned
     *          set of permissions must be a new mutable instance
     *          and it must support heterogeneous Permission types.
     *          If this operation is not supported,
     *          Policy.EMPTY_PERMISSION_COLLECTION is returned.
     */
    public PermissionCollection getPermissions(ProtectionDomain domain) {
        PermissionCollection pc = null;

        if (domain == null) return new Permissions();

        if (pdMapping == null) {
            initPolicy(this);
        }

        synchronized (pdMapping) {
            pc = pdMapping.get(domain.getCodeSource().hashCode());
        }

        if (pc != null) {
            Permissions perms = new Permissions();
            synchronized (pc) {
                for (Enumeration<Permission> e = pc.elements(); e.hasMoreElements();) {
                    perms.add(e.nextElement());
                }
            }
            return perms;
        }

        pc = getPermissions(domain.getCodeSource());
        if (pc == null || pc == EMPTY_PERMISSION_COLLECTION) {
            pc = new Permissions();
        }

        addStaticPerms(pc, domain.getPermissions());
        return pc;
    }

    /**
     * add static permissions to provided permission collection
     */
    private void addStaticPerms(PermissionCollection perms, PermissionCollection statics) {
        if (statics != null) {
            synchronized (statics) {
                Enumeration<Permission> e = statics.elements();
                while (e.hasMoreElements()) {
                    perms.add(e.nextElement());
                }
            }
        }
    }

    /**
     * Evaluates the global policy for the permissions granted to
     * the ProtectionDomain and tests whether the permission is
     * granted.
     *
     * @param domain the ProtectionDomain to test
     * @param permission the Permission object to be tested for implication.
     *
     * @return {@code true} if "permission" is a proper subset of a permission
     * granted to this ProtectionDomain.
     *
     * @see java.security.ProtectionDomain
     */
    public boolean implies(ProtectionDomain domain, Permission permission) {
        PermissionCollection pc;

        if (pdMapping == null) {
            initPolicy(this);
        }

        synchronized (pdMapping) {
            pc = pdMapping.get(domain.getCodeSource().hashCode());
        }

        if (pc != null) {
            return pc.implies(permission);
        }

        pc = getPermissions(domain);
        if (pc == null) {
            return false;
        }

        synchronized (pdMapping) {
            // cache it
            pdMapping.put(domain.getCodeSource().hashCode(), pc);
        }

        return pc.implies(permission);
    }

    /**
     * Refreshes/reloads the policy configuration. The behavior of this method
     * depends on the implementation. For example, calling {@code refresh}
     * on a file-based policy will cause the file to be re-read.
     *
     * <p> The default implementation of this method does nothing.
     * This method should be overridden if a refresh operation is supported
     * by the policy implementation.
     */
    public void refresh() {}

    /**
     * This class represents a read-only empty PermissionCollection object that
     * is returned from the {@code getPermissions(CodeSource)} and
     * {@code getPermissions(ProtectionDomain)}
     * methods in the {@code Policy} class when those operations are not
     * supported by the Policy implementation.
     */
    private static class EmptyPermissionCollection extends PermissionCollection {

        @java.io.Serial
        private static final long serialVersionUID = -8492269157353014774L;

        private Permissions perms;

        /**
         * Create a read-only empty PermissionCollection object.
         */
        public EmptyPermissionCollection() {
            this.perms = new Permissions();
            perms.setReadOnly();
        }

        /**
         * Adds a permission object to the current collection of permission
         * objects.
         *
         * @param permission the Permission object to add.
         *
         * @throws    SecurityException   if this PermissionCollection object
         *                                has been marked readonly
         */
        @Override
        public void add(Permission permission) {
            perms.add(permission);
        }

        /**
         * Checks to see if the specified permission is implied by the
         * collection of Permission objects held in this PermissionCollection.
         *
         * @param permission the Permission object to compare.
         *
         * @return {@code true} if "permission" is implied by the permissions in
         * the collection, {@code false} if not.
         */
        @Override
        public boolean implies(Permission permission) {
            return perms.implies(permission);
        }

        /**
         * Returns an enumeration of all the Permission objects in the
         * collection.
         *
         * @return an enumeration of all the Permissions.
         */
        @Override
        public Enumeration<Permission> elements() {
            return perms.elements();
        }
    }
}
