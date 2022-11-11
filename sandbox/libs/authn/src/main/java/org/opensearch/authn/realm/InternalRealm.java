/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.authn.User;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * Internal Realm is a custom realm using the internal OpenSearch IdP
 *
 * @opensearch.experimental
 */
public class InternalRealm extends AuthenticatingRealm { //This overrides do get authentication info
    private static final String DEFAULT_REALM_NAME = "internal";

    private static final String DEFAULT_INTERNAL_USERS_FILE = "example/example_internal_users.yml";

    public static final InternalRealm INSTANCE = new InternalRealm.Builder(DEFAULT_REALM_NAME, DEFAULT_INTERNAL_USERS_FILE).build();

    private final String realmName;

    private ConcurrentMap<String, User> internalUsers;

    private InternalRealm(String realmName, ConcurrentMap<String, User> internalUsers) {
        super(new BCryptPasswordMatcher());
        this.realmName = realmName;
        this.internalUsers = internalUsers;
    }

    public static final class Builder {
        private final String name;

        private final String pathToInternalUsersYaml;

        public Builder(String name, String pathToInternalUsersYaml) {
            this.name = Objects.requireNonNull(name);
            this.pathToInternalUsersYaml = pathToInternalUsersYaml;
        }

        public InternalRealm build() {
            ConcurrentMap<String, User> internalUsers = InternalUsersStore.readInternalSubjectsAsMap(pathToInternalUsersYaml);
            return new InternalRealm(name, internalUsers);
        }
    }

    private void initializeInternalSubjectsStore(String pathToInternalUsersYaml) {
        // TODO load this at cluster start
        internalUsers = InternalUsersStore.readInternalSubjectsAsMap(pathToInternalUsersYaml);
    }

    public User getInternalUser(String principalIdentifier) throws UnknownAccountException {
        User userRecord = internalUsers.get(principalIdentifier);
        // UserRecord userRecord = lookupUserRecord(username);
        // No record found - don't know who this is
        if (userRecord == null) {
            throw new UnknownAccountException(principalIdentifier + " does not exist in " + realmName + " realm.");
        }
        return userRecord;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (token instanceof UsernamePasswordToken) { //This handles Username password token so need to add logic for Bearer token -- extract subject --> get user record w/ getInternalUser
            String username = ((UsernamePasswordToken) token).getUsername();
            final char[] password = ((UsernamePasswordToken) token).getPassword();
            // Look up the user by the provide username
            User userRecord = getInternalUser(username);
            // Check for other things, like a locked account, expired password, etc.

            // Verify the user
            SimpleAuthenticationInfo sai = new SimpleAuthenticationInfo(
                userRecord.getPrimaryPrincipal(),
                userRecord.getBcryptHash(),
                realmName
            );
            boolean successfulAuthentication = getCredentialsMatcher().doCredentialsMatch(token, sai); // Can move JWT verification logic into here


            // TODO: Need to add similar functionality for bearer tokens
            // Login function itself does the verification so you could move verification into here because of doGetLogin leading to here



            if (successfulAuthentication) {
                // Check for anything else that might prevent login (expired password, locked account, etc
                // if (other problems) {
                // throw new CredentialsException(); // Or something more specific
                // }
                // Success!
                return sai;
            } else {
                // Bad password
                throw new IncorrectCredentialsException("Incorrect credentials");
            }
        }
        // Don't know what to do with this token
        throw new CredentialsException();
    }
}
