package org.opensearch.identity;

import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.AllPermission;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.SimpleAccountRealm;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;

import java.util.Collection;
import java.util.Collections;

/**
 * Nothing yet
 */
public class MyShiroModule {

    public MyShiroModule() {
        final Realm realm = new MyRealm();

        final SecurityManager securityManager = new DefaultSecurityManager(realm);

        SecurityUtils.setSecurityManager(securityManager);
    }

    /**
     * Generates an internal subject to run requests in this context
     */
    public static Subject getSubjectOrInternal() {
        final Subject existingSubject = SecurityUtils.getSubject();
        if (existingSubject.isAuthenticated()) {
            return existingSubject;
        }

        // How do we impart permissions on this subject?
        final Subject internalSubject = new Subject.Builder()
            .authenticated(true)
            .principals(new SimplePrincipalCollection("INTERNAL", "OpenSearch"))
            .buildSubject();
        return internalSubject;
    }

    private enum Roles {
        ALL_ACCESS,
        CLUSTER_MONITOR;
    }
    private static class MyRealm extends SimpleAccountRealm {
        private MyRealm() {
            super("OpenSearch");

            this.addAccount("admin", "admin", Roles.ALL_ACCESS.name());
            this.addAccount("user", "user");

            this.addAccount("INTERNAL", "INTERNAL", Roles.CLUSTER_MONITOR.name());

            this.setRolePermissionResolver(new RolePermissionResolver() {
                @Override
                public Collection<Permission> resolvePermissionsInRole(final String roleString) {
                    switch (Roles.valueOf(roleString)) {
                        case ALL_ACCESS:
                            return Collections.singleton(new AllPermission());
                        case CLUSTER_MONITOR:
                            return Collections.singleton(new WildcardPermission("cluster:monitor"));
                        default:
                            throw new RuntimeException("Unknown Permission: " + roleString);
                    }
                }
            });
        }


    }
}
