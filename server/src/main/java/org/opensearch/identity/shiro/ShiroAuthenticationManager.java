package org.opensearch.identity.shiro;

import java.security.Principal;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.List;
import java.util.Collections;

import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.opensearch.common.Strings;
import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.AuthenticationToken;
import org.opensearch.identity.PermissionResult;
import org.opensearch.identity.AuthenticationSession;
import org.opensearch.identity.HttpHeaderToken;
import org.opensearch.identity.Subject;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import java.util.Base64;

/** Authentication manager using Shiro */
public class ShiroAuthenticationManager implements AuthenticationManager {

    private static final Logger LOGGER = LogManager.getLogger(ShiroAuthenticationManager.class);

    private final List<HeaderAuthorizer> authorizers;

    public ShiroAuthenticationManager() {
        // Ensure the shiro system configuration has been performed
        new MyShiroModule();

        // TODO: Dynamically configure authentication sources
        authorizers = Collections.singletonList(new ShiroBasicHeaderAuthorizer());
    }

    @Override
    public Subject getSubject() {
        return new ShiroSubject(SecurityUtils.getSubject());
    }

    @Override
    public void login(final AuthenticationToken token) {
        if (token instanceof HttpHeaderToken) {
            loginWith((HttpHeaderToken)token);
        } else {
            throw new RuntimeException("Unsupported token type, " + token.getClass().getSimpleName());
        }
    }

    public void loginWith(final HttpHeaderToken token) {
        // Iterate over the available authorizers to attempt to authorize the user
        for (final HeaderAuthorizer authorizer : authorizers) {
            if (authorizer.authorizeWithHeader(token.getHeaderValue())) {
                return;
            }
        }
        throw new RuntimeException("Unable to authenticate user!");
    }

    public Runnable systemLogin(final Runnable runnable, final String systemResource) {
        final org.apache.shiro.subject.Subject internalSubject = new org.apache.shiro.subject.Subject.Builder().authenticated(true)
        .principals(new SimplePrincipalCollection("System-" + systemResource, "OpenSearch")) // How can we ensure the roles this
                                                                                        // principal resolves?
        .buildSubject();

        return internalSubject.associateWith(runnable);
    }

    public AuthenticationSession dangerousAuthenticateAs(final String subject) {
        final org.apache.shiro.subject.Subject internalSubject = new org.apache.shiro.subject.Subject.Builder().authenticated(true)
            .principals(new SimplePrincipalCollection("INTERNAL-" + subject, "OpenSearch")) // How can we ensure the roles this
                                                                                            // principal resolves?
            .contextAttribute("NodeId", "???") // Can we use this to source the originating node in a cluster?
            .buildSubject();

        final SubjectThreadState threadState = new SubjectThreadState(internalSubject);
        threadState.bind();

        
        return closeAuthenticateAsSession(threadState, subject);
    }

    private AuthenticationSession closeAuthenticateAsSession(final SubjectThreadState threadState, final String subjectReference) {
        return () -> {
            try {
                threadState.clear();
            } catch (final Exception e) {
                LOGGER.error("Unable to close authentication session as {}", subjectReference, e);
            }
        };
    }

    /**
     * Wraps Shiro's Subject implementation
     */
    private static class ShiroSubject implements Subject {

        private final org.apache.shiro.subject.Subject internalSubject;

        public ShiroSubject(org.apache.shiro.subject.Subject subject) {
            this.internalSubject = subject;
        }

        @Override
        public Principal getPrincipal() {
            final Object o = internalSubject.getPrincipal();

            if (o == null) {
                return null;
            }

            if (o instanceof Principal) {
                return (Principal) o;
            }

            return new Principal() {
                @Override
                public String getName() {
                    return o.toString();
                }
            };
        }

        @Override
        public PermissionResult isPermitted(String permissionName) {
            final boolean isPermitted = internalSubject.isPermitted(permissionName);
            final Supplier<String> errorMessage = () -> "Unable to permit user '" + this.toString() + "', for permission " + permissionName;
            return new ShiroPermissionResult(isPermitted, errorMessage);
        }

        @Override
        public String toString() {
            return this.getPrincipal() != null ? this.getPrincipal().getName() : "No Subject";
        }
    }

    private static class ShiroPermissionResult implements PermissionResult {
        private final boolean isAllowed;
        private final Supplier<String> message;

        public ShiroPermissionResult(final boolean isAllowed, final Supplier<String> message) {
            this.isAllowed = isAllowed;
            this.message = message;
        }

        @Override
        public boolean isAllowed() {
            return isAllowed;
        }

        @Override
        public String getErrorMessage() {
            return message.get();
        }
    }

    /**
     * Authorizes a user from a HTTP Header
     */
    private interface HeaderAuthorizer {

        /**
         * Attempt to authorize the user
         */
        public boolean authorizeWithHeader(final String authHeader);
    }

    /**
     * THROW AWAY IMPLEMENTATION
     */
    private static class ShiroBasicHeaderAuthorizer implements HeaderAuthorizer {
        public boolean authorizeWithHeader(final String authHeader) {
            if (!(Strings.isNullOrEmpty(authHeader)) && authHeader.startsWith("Basic")) {
                final byte[] decodedAuthHeader = Base64.getDecoder().decode(authHeader.substring("Basic".length()).trim());
                final String[] decodedUserNamePassword = new String(decodedAuthHeader).split(":");
                final org.apache.shiro.subject.Subject currentSubject = SecurityUtils.getSubject();
                currentSubject.login(new UsernamePasswordToken(decodedUserNamePassword[0], decodedUserNamePassword[1]));
                LOGGER.info("Authenticated user '{}'", currentSubject.getPrincipal());
                return true;
            }
            return false;
        }
    }
}
