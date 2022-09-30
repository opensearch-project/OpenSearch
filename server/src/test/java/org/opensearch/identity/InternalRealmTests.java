/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.opensearch.identity.realm.InternalRealm;
import org.opensearch.test.OpenSearchTestCase;

import java.io.FileNotFoundException;

public class InternalRealmTests extends OpenSearchTestCase {

    public void testGetAuthenticationInfoUserExists() throws FileNotFoundException {
        String username = "admin";
        String password = "admin";
        String internalUserTestFile = "config/internal_users_test.yml";
        UsernamePasswordToken token = new UsernamePasswordToken(username, password);
        InternalRealm realm = InternalRealm.INSTANCE;
        realm.initializeInternalSubjectsStore(internalUserTestFile);
        InternalSubject admin = realm.internalSubjects.get("admin");
        AuthenticationInfo adminInfo = realm.getAuthenticationInfo(token);
        assertNotNull(adminInfo);
    }

    public void testGetAuthenticationInfoUserExistsWrongPassword() throws FileNotFoundException {
        String username = "admin";
        String password = "wrong_password";
        String internalUserTestFile = "config/internal_users_test.yml";
        UsernamePasswordToken token = new UsernamePasswordToken(username, password);
        InternalRealm realm = InternalRealm.INSTANCE;
        realm.initializeInternalSubjectsStore(internalUserTestFile);
        InternalSubject admin = realm.internalSubjects.get("admin");
        try {
            AuthenticationInfo adminInfo = realm.getAuthenticationInfo(token);
            fail("Expected to throw IncorrectCredentialsException");
        } catch (AuthenticationException e) {
            assertTrue(e instanceof IncorrectCredentialsException);
        }
    }
}
