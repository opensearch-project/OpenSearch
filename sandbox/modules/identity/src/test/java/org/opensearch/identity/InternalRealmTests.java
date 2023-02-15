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
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.configuration.model.InternalUsersModel;
import org.opensearch.identity.realm.InternalRealm;
import org.opensearch.identity.realm.InternalUsersStore;
import org.opensearch.test.OpenSearchTestCase;

public class InternalRealmTests extends OpenSearchTestCase {

    private InternalRealm realm;

    private InternalUsersModel ium;

    @Before
    public void setUpAndInitializeRealm() throws Exception {
        SecurityDynamicConfiguration<User> internalUsers = SecurityDynamicConfiguration.empty();
        User user1 = new User("admin", "$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG", null, null);
        internalUsers.putCObject("admin", user1);
        InternalUsersModel testModel = new InternalUsersModel(internalUsers);

        InternalUsersStore.getInstance(); // Instantiates InternalUsersStore
        Thread.sleep(100);
        realm = new InternalRealm.Builder("test").build();
        InternalUsersStore.getInstance().onInternalUsersModelChanged(testModel);
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
