/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.identity.realm.InternalUsersStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class InternalUsersStoreTests extends OpenSearchTestCase {

    public void testReadInternalSubjectsTest() {
        String internalSubjectsYaml = "internal_users_test.yml";
        Map<String, User> internalUserMap = InternalUsersStore.readUsersAsMap(internalSubjectsYaml);
        assertTrue(internalUserMap.containsKey("new-user"));
        assertFalse(internalUserMap.containsKey("new-user2"));
    }

    public void testReadInternalSubjectsFileDoesNotExistTest() {
        String internalSubjectsYaml = "config/does_not_exist.yml";
        try {
            Map<String, User> internalUserMap = InternalUsersStore.readUsersAsMap(internalSubjectsYaml);
            fail("Expected to throw FileNotFoundException");
        } catch (RuntimeException e) {
            // expected
        }
    }
}
