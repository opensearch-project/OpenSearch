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
import org.junit.Before;
import org.opensearch.identity.configuration.model.InternalUsersModel;
import org.opensearch.identity.realm.InternalRealm;
import org.opensearch.identity.realm.InternalUsersStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class InternalRealmTests extends OpenSearchTestCase {

    private InternalRealm realm;

    private class TestInternalUsersModel extends InternalUsersModel {

        private User user1 = new User("admin", "$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG", null);
        private final Map<String, User> userModel = Map.of("admin", user1);

        @Override
        public User getUser(String username) {
            return userModel.get(username);
        }

        @Override
        public boolean exists(String username) {
            return userModel.containsKey(username);
        }

        @Override
        public Map<String, String> getAttributes(String username) {
            User tmp = userModel.get(username);
            return tmp == null ? null : tmp.getAttributes();
        }

        @Override
        public String getHash(String username) {
            User tmp = userModel.get(username);
            return tmp == null ? null : tmp.getBcryptHash();
        }
    }

    private TestInternalUsersModel ium = new TestInternalUsersModel();

    @Before
    public void setUpAndInitializeRealm() throws Exception {
        InternalUsersStore.getInstance(); // Instantiates InternalUsersStore
        Thread.sleep(100);
        realm = new InternalRealm.Builder("test").build();
        InternalUsersStore.getInstance().onInternalUsersModelChanged(ium);
    }

    public void testGetAuthenticationInfoUserExists() {
        String username = "admin";
        String password = "admin";
        UsernamePasswordToken token = new UsernamePasswordToken(username, password);
        User admin = realm.getInternalUser("admin");
        AuthenticationInfo adminInfo = realm.getAuthenticationInfo(token);
        assertNotNull(adminInfo);
    }

    public void testGetAuthenticationInfoUserExistsWrongPassword() {
        String username = "admin";
        String password = "wrong_password";
        UsernamePasswordToken token = new UsernamePasswordToken(username, password);
        User admin = realm.getInternalUser("admin");
        try {
            AuthenticationInfo adminInfo = realm.getAuthenticationInfo(token);
            fail("Expected to throw IncorrectCredentialsException");
        } catch (AuthenticationException e) {
            assertTrue(e instanceof IncorrectCredentialsException);
        }
    }
}
