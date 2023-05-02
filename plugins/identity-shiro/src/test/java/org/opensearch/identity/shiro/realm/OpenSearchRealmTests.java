/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro.realm;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

public class OpenSearchRealmTests extends OpenSearchTestCase {

    private OpenSearchRealm realm;

    @Before
    public void setup() {
        realm = new OpenSearchRealm.Builder("test").build();
    }

    public void testGetAuthenticationInfoUserExists() {
        final UsernamePasswordToken token = new UsernamePasswordToken("admin", "admin");
        final User admin = realm.getInternalUser("admin");
        final AuthenticationInfo adminInfo = realm.getAuthenticationInfo(token);
        assertNotNull(adminInfo);
    }

    public void testGetAuthenticationInfoUserExistsWrongPassword() {
        final UsernamePasswordToken token = new UsernamePasswordToken("admin", "wrong_password");
        final User admin = realm.getInternalUser("admin");

        assertThrows(IncorrectCredentialsException.class, () -> realm.getAuthenticationInfo(token));
    }
}
