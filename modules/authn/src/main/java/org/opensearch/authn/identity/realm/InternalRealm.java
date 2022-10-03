/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.identity.realm;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.CredentialsException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.realm.AuthenticatingRealm;
import org.opensearch.authn.identity.InternalSubject;

import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Internal Realm is a custom realm using the internal OpenSearch IdP
 *
 * @opensearch.experimental
 */
public class InternalRealm extends AuthenticatingRealm {

    public static final InternalRealm INSTANCE = new InternalRealm();
    private static final String REALM_NAME = "internal";

    private boolean isRealmInitialized = false;

    public InternalRealm() {
        super(new BCryptPasswordMatcher());
    }

    // TODO Switch this to private after debugging
    public Map<String, InternalSubject> internalSubjects;

    public void initializeInternalSubjectsStore(String pathToInternalUsersYaml) throws FileNotFoundException {
        // TODO load this at cluster start
        internalSubjects = InternalSubjectsStore.readInternalSubjectsAsMap(pathToInternalUsersYaml);
        isRealmInitialized = true;
    }

    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {
        if (!isRealmInitialized || internalSubjects == null) {
            // nothing to do
            return null;
        }
        if (token instanceof UsernamePasswordToken) {
            String username = ((UsernamePasswordToken) token).getUsername();
            final char[] password = ((UsernamePasswordToken) token).getPassword();
            // Look up the user by the provide username
            InternalSubject userRecord = internalSubjects.get(username);
            // UserRecord userRecord = lookupUserRecord(username);
            // No record found - don't know who this is
            if (userRecord == null) {
                throw new UnknownAccountException();
            }
            // Check for other things, like a locked account, expired password, etc.

            // Verify the user
            SimpleAuthenticationInfo sai = new SimpleAuthenticationInfo(userRecord.getPrimaryPrincipal(), userRecord.getHash(), REALM_NAME);
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
