/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.realm;

import org.apache.cxf.rs.security.jose.jwt.JwtToken;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.BearerToken;
import org.apache.shiro.authc.CredentialsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.SimpleAuthorizationInfo;

import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.opensearch.authn.StringPrincipal;
import org.opensearch.identity.User;
import org.opensearch.identity.authz.OpenSearchPermission;
import org.opensearch.identity.configuration.model.InternalUsersModel;
import org.opensearch.identity.jwt.BadCredentialsException;
import org.opensearch.identity.jwt.JwtVerifier;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Internal Realm is a custom realm using the internal OpenSearch IdP
 *
 * @opensearch.experimental
 */
public class InternalRealm extends AuthorizingRealm {

    public static final String INCORRECT_CREDENTIALS_MESSAGE = "Incorrect credentials";

    private static final String DEFAULT_REALM_NAME = "internal";

    private String realmName;

    private InternalRealm(String realmName) {
        super(new BCryptPasswordMatcher());
        this.realmName = realmName;
    }

    public InternalRealm() {
        super(new BCryptPasswordMatcher());
    }

    public static final class Builder {
        private final String name;

        public Builder(String name) {
            this.name = Objects.requireNonNull(name);
        }

        public InternalRealm build() {
            return new InternalRealm(name);
        }
    }

    public User getInternalUser(String principalIdentifier) throws UnknownAccountException {
        InternalUsersModel internalUsersModel = InternalUsersStore.getInstance().getInternalUsersModel();
        Objects.requireNonNull(internalUsersModel);
        User userRecord = internalUsersModel.getUser(principalIdentifier);
        // UserRecord userRecord = lookupUserRecord(username);
        // No record found - don't know who this is
        if (userRecord == null) {
            throw new UnknownAccountException(principalIdentifier + " does not exist in " + realmName + " realm.");
        }
        return userRecord;
    }

    // TODO: Revisit this
    // This was overridden to support all kinds of AuthTokens
    @Override
    public boolean supports(AuthenticationToken token) {
        return true;
    }

    @Override
    protected void assertCredentialsMatch(AuthenticationToken token, AuthenticationInfo info) throws AuthenticationException {
        if (token instanceof BearerToken) {
            // TODO: Check if this is correct
            // Token has previously been verified at doGetAuthenticationInfo
            // no auth required as bearer token is assumed to have correct credentials
        } else if (token instanceof UsernamePasswordToken) {
            super.assertCredentialsMatch(token, info); // continue as normal for basic-auth token
        }
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof UsernamePasswordToken) {
            String username = ((UsernamePasswordToken) token).getUsername();
            // Look up the user by the provide username
            User userRecord = getInternalUser(username);
            // Check for other things, like a locked account, expired password, etc.

            // Verify the user
            // TODO Figure out why userRecord is coming back with empty username
            userRecord.setUsername(new StringPrincipal(username));
            SimpleAuthenticationInfo sai = new SimpleAuthenticationInfo(username, userRecord.getHash(), realmName);
            boolean successfulAuthentication = getCredentialsMatcher().doCredentialsMatch(token, sai);

            if (successfulAuthentication) {
                // Check for anything else that might prevent login (expired password, locked account, etc.)
                // if (other problems) {
                // throw new CredentialsException(); // Or something more specific
                // }
                // Success!
                return sai;
            } else {
                // Bad password
                throw new IncorrectCredentialsException(INCORRECT_CREDENTIALS_MESSAGE);
            }
        } else if (token instanceof BearerToken) {
            JwtToken jwtToken;

            // Verify the validity of JWT token
            try {
                jwtToken = JwtVerifier.getVerifiedJwtToken(((BearerToken) token).getToken());
            } catch (BadCredentialsException e) {
                throw new IncorrectCredentialsException(e.getMessage()); // Invalid Token
            }

            String subject = jwtToken.getClaims().getSubject();

            // We need to extract the subject here to create an identity subject that can be utilized across the realm
            return new SimpleAuthenticationInfo(subject, null, realmName);
        }
        // Don't know what to do with this token
        throw new CredentialsException();
    }

    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {
        SimpleAuthorizationInfo authorizations = new SimpleAuthorizationInfo();
        if (principals == null || principals.isEmpty()) {
            return authorizations;
        }
        String username = (String) principals.getPrimaryPrincipal();
        User userRecord = getInternalUser(username);
        Set<Permission> permissions = userRecord.getPermissions()
            .stream()
            .map(p -> new OpenSearchPermission(p))
            .collect(Collectors.toSet());
        authorizations.setObjectPermissions(permissions);
        return authorizations;
    }

    public void setRealmName(String realmName) {
        this.realmName = realmName;
    }
}
