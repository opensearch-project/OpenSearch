/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.realm;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.CredentialsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.realm.AuthenticatingRealm;
import org.opensearch.authn.InternalSubject;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * Internal Realm is a custom realm using the internal OpenSearch IdP
 *
 * @opensearch.experimental
 */
public class InternalRealm extends AuthenticatingRealm {
    private static final String DEFAULT_REALM_NAME = "internal";

    public static final InternalRealm INSTANCE = new InternalRealm.Builder(DEFAULT_REALM_NAME).build();

    private final String realmName;

    private InternalRealm(String realmName) {
        super(new BCryptPasswordMatcher());
        this.realmName = realmName;
    }

    // TODO Switch this to private after debugging
    public ConcurrentMap<String, InternalSubject> internalSubjects;

    public static final class Builder {
        private final String name;

        public Builder(String name) {
            this.name = Objects.requireNonNull(name);
        }

        public InternalRealm build() {
            return new InternalRealm(name);
        }
    }

    public void initializeInternalSubjectsStore(String pathToInternalUsersYaml) {
        // TODO load this at cluster start
        internalSubjects = InternalSubjectsStore.readInternalSubjectsAsMap(pathToInternalUsersYaml);
    }

    public InternalSubject getInternalSubject(String principalIdentifier) throws UnknownAccountException {
        InternalSubject userRecord = internalSubjects.get(principalIdentifier);
        // UserRecord userRecord = lookupUserRecord(username);
        // No record found - don't know who this is
        if (userRecord == null) {
            throw new UnknownAccountException(principalIdentifier + " does not exist in " + realmName + " realm.");
        }
        return userRecord;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof UsernamePasswordToken) {
            String username = ((UsernamePasswordToken) token).getUsername();
            final char[] password = ((UsernamePasswordToken) token).getPassword();
            // Look up the user by the provide username
            InternalSubject userRecord = getInternalSubject(username);
            // Check for other things, like a locked account, expired password, etc.

            // Verify the user
            SimpleAuthenticationInfo sai = new SimpleAuthenticationInfo(
                userRecord.getPrimaryPrincipal(),
                userRecord.getBcryptHash(),
                realmName
            );
            boolean successfulAuthentication = getCredentialsMatcher().doCredentialsMatch(token, sai);

            if (successfulAuthentication) {
                // Check for anything else that might prevent login (expired password, locked account, etc
                // if (other problems) {
                // throw new CredentialsException(); // Or something more specific
                // }
                // Success!
                return sai;
            } else {
                // Bad password
                throw new IncorrectCredentialsException();
            }
        }
        // Don't know what to do with this token
        throw new CredentialsException();
    }
}
