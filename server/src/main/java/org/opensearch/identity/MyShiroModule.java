package org.opensearch.identity;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.AllPermission;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.SimpleAccountRealm;
import org.apache.shiro.mgt.DefaultSecurityManager;

import java.util.Collection;
import java.util.Collections;

/**
 * Prototyping use of identity management (authentication and authorization) implemented with Shiro
 * See more at https://shiro.apache.org/
 */
public class MyShiroModule {

    public MyShiroModule() {
        final Realm realm = new MyRealm();
        final SecurityManager securityManager = new DefaultSecurityManager(realm);
        // Note; this sets up security for the JVM, if we are crossing a JVM boundary will need to look at how this is made available
        SecurityUtils.setSecurityManager(securityManager);
    }

    /* Super basic role management */
    private enum Roles {
        ALL_ACCESS,
        CLUSTER_MONITOR;
    }

    /** Very basic user pool and permissions ecosystem */
    private static class MyRealm extends SimpleAccountRealm {
        private MyRealm() {
            super("OpenSearch");

            /* Default account configuration */
            this.addAccount("admin", "admin", Roles.ALL_ACCESS.name());
            this.addAccount("user", "user");

            /* Attempt to grant access for internal accounts, but wasn't able to correlate them via
              the Just-In-Time subject creation, will need to do additional investigation */
            this.addAccount("INTERNAL", "INTERNAL", Roles.CLUSTER_MONITOR.name());

            /* Looking at how roles can be translated into permissions */
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
