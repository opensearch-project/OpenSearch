/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.apache.shiro.authc.UnknownAccountException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.junit.Before;
import org.opensearch.authn.realm.InternalRealm;
import org.opensearch.test.OpenSearchTestCase;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;

public class InternalRealmTests extends OpenSearchTestCase {

    private InternalRealm realm;

    @Before
    public void setUpAndInitializeRealm() throws FileNotFoundException {
        realm = new InternalRealm.Builder("test", "internal_users_test.yml").build();
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

    public void testCreateUser() {
        String otherPrimaryPrincipal = "other_principal";

        String primaryPrincipal = "some_principal";
        String hash = "some_hash";
        Map<String, String> attributes = Map.of("attribute1", "val1", "attribute2", "val2");

        realm.createUser(primaryPrincipal, hash, attributes);

        assertEquals(primaryPrincipal, realm.getInternalUser(primaryPrincipal).getPrimaryPrincipal().getName());

        User subjectToBeCreated = new User();
        subjectToBeCreated.setAttributes(attributes);
        subjectToBeCreated.setBcryptHash(hash);
        subjectToBeCreated.setPrimaryPrincipal(new StringPrincipal(otherPrimaryPrincipal));

        realm.createUser(subjectToBeCreated);

        assertEquals(primaryPrincipal, realm.getInternalUser(otherPrimaryPrincipal).getPrimaryPrincipal().getName());
    }

    public void testUpdateUserPassword() {
        String primaryPrincipal = "new-user";
        String currentHash = "$2y$12$88IFVl6IfIwCFh5aQYfOmuXVL9j2hz/GusQb35o.4sdTDAEMTOD.K";
        User newUser = realm.getInternalUser(primaryPrincipal);

        assertEquals(currentHash, newUser.getBcryptHash());

        String newHash = "new_hash";
        realm.updateUserPassword(primaryPrincipal, newHash);
        String newUserPasswordHash = realm.getInternalUser(primaryPrincipal).getBcryptHash();
        assertEquals(newHash, newUserPasswordHash);
    }

    public void testAddNewAttributesToUser() {
        String primaryPrincipal = "new-user";
        User newUser = realm.getInternalUser(primaryPrincipal);

        Map<String, String> newUserAttributes = newUser.getAttributes();

        assertEquals(1, newUserAttributes.size());
        assertFalse(newUserAttributes.containsKey("attr2"));

        Map<String, String> newAttributes = Map.of("attr2", "val2", "attr3", "val3");

        realm.updateUserAttributes(primaryPrincipal, newAttributes);

        Map<String, String> updatedAttributes = realm.getInternalUser(primaryPrincipal).getAttributes();
        assertEquals(3, updatedAttributes.size());
        assertTrue(newUserAttributes.containsKey("attr2"));
    }

    public void testDeleteAttributesFromUser() {

        String primaryPrincipal = "new-user";
        User newUser = realm.getInternalUser(primaryPrincipal);

        Map<String, String> newUserAttributes = newUser.getAttributes();

        assertEquals(1, newUserAttributes.size());
        assertTrue(newUserAttributes.containsKey("attribute1"));

        // attribute2 doesn't exist in the new-user's map of attributes, but doesn't matter
        List<String> attributesToBeDeleted = List.of("attribute1", "attribute2");

        realm.deleteAttributesFromUser(primaryPrincipal, attributesToBeDeleted);

        Map<String, String> updatedAttributes = realm.getInternalUser(primaryPrincipal).getAttributes();
        assertEquals(0, updatedAttributes.size());
    }

    public void testDeleteUser() {
        String primaryPrincipal = "new-user";
        assertEquals(primaryPrincipal, realm.getInternalUser(primaryPrincipal).getPrimaryPrincipal().getName());
        realm.deleteUser(primaryPrincipal);
        assertThrows(UnknownAccountException.class, () -> realm.getInternalUser(primaryPrincipal));
    }
}
