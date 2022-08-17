package org.opensearch.identity;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.AllPermission;
import org.apache.shiro.authz.permission.RolePermissionResolver;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.SimpleAccountRealm;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.opensearch.common.Strings;

import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Prototyping use of security system implemented with Shiro
 * See more at https://shiro.apache.org/
 */
public class MyShiroModule {

    public MyShiroModule() {
        final Realm realm = new MyRealm();
        final SecurityManager securityManager = new DefaultSecurityManager(realm);
        // Note; this sets up security for the JVM, if we are crossing a JVM boundary will need to look at how this is made available
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

        // Used for tracing where this function is used that is visible in the log output
        final Exception e = new Exception();
        final StackTraceElement current = e.getStackTrace()[1];
        final String sourceAnnotation = current.getFileName() + "." + current.getMethodName() + "@" + current.getLineNumber();

        final Subject internalSubject = new Subject.Builder().authenticated(true)
            .principals(new SimplePrincipalCollection("INTERNAL-" + sourceAnnotation, "OpenSearch")) // How can we ensure the roles this
                                                                                                     // princpal resolves?
            .contextAttribute("NodeId", "???") // Can we use this to source the originating node in a cluster?
            .buildSubject();
        return internalSubject;
    }

    /**
     * Attempt to authenticate via authorization header, ignores if already authenticated, throws exceptions if unable
     *
     * NOTE: this was quickly built for test scenarios and will not be used log term, there is a supplimental library for
     * Shiro that supports web based applications, including https://shiro.apache.org/static/1.9.1/apidocs/org/apache/shiro/web/filter/authc/BasicHttpAuthenticationFilter.html
     * if this can be reused and to what degree will be useful in future investigations.
     */
    public static void authenticateViaAuthorizationHeader(final Optional<String> authHeader) throws AuthenticationException {
        final Subject currentSubject = SecurityUtils.getSubject();
        if (currentSubject.isAuthenticated()) {
            // No need to authenticate already ready
            return;
        }

        if (authHeader.isPresent() && !(Strings.isNullOrEmpty(authHeader.get())) && authHeader.get().startsWith("Basic ")) {
            final byte[] decodedAuthHeader = Base64.getDecoder().decode(authHeader.get().substring(6));
            final String[] decodedUserNamePassword = new String(decodedAuthHeader).split(":");
            currentSubject.login(new UsernamePasswordToken(decodedUserNamePassword[0], decodedUserNamePassword[1]));
            // Successful login - return!
            return;
        }

        throw new AuthenticationException("Unable to authenticate user!");
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
