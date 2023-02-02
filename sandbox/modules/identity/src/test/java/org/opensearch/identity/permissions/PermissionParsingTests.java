/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.permissions;

import org.opensearch.identity.User;
import org.opensearch.identity.authz.OpenSearchPermission;
import org.opensearch.identity.authz.PermissionFactory;
import org.opensearch.identity.authz.PermissionStorage;
import org.opensearch.identity.realm.InternalUsersStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class PermissionParsingTests extends OpenSearchTestCase{

    public void testParsePermissionTest() {
        String principalString = "test_user";
        String permissionString = "cluster:admin/read";
        OpenSearchPermission newPermission = PermissionFactory.createPermission(permissionString);
        PermissionStorage.put(principalString, permissionString);
        assertTrue(internalUserMap.containsKey("new-user"));
        assertFalse(internalUserMap.containsKey("new-user2"));
    }

    public void testParseMalformedPermissionTest() {
        String internalSubjectsYaml = "internal_users_test.yml";
        Map<String, User> internalUserMap = InternalUsersStore.readUsersAsMap(internalSubjectsYaml);
        assertTrue(internalUserMap.containsKey("new-user"));
        assertFalse(internalUserMap.containsKey("new-user2"));
    }

    public void testParseWildcardPermissionTest() {
        String internalSubjectsYaml = "internal_users_test.yml";
        Map<String, User> internalUserMap = InternalUsersStore.readUsersAsMap(internalSubjectsYaml);
        assertTrue(internalUserMap.containsKey("new-user"));
        assertFalse(internalUserMap.containsKey("new-user2"));
    }

    public void testParseNegatedPermissionTest() {
        String internalSubjectsYaml = "config/does_not_exist.yml";
        try {
            Map<String, User> internalUserMap = InternalUsersStore.readUsersAsMap(internalSubjectsYaml);
            fail("Expected to throw FileNotFoundException");
        } catch (RuntimeException e) {
            // expected
        }
    }

    public void testParseWildcardAndNegatedPermissionTest() {
        String internalSubjectsYaml = "internal_users_test.yml";
        Map<String, User> internalUserMap = InternalUsersStore.readUsersAsMap(internalSubjectsYaml);
        assertTrue(internalUserMap.containsKey("new-user"));
        assertFalse(internalUserMap.containsKey("new-user2"));
    }
}


