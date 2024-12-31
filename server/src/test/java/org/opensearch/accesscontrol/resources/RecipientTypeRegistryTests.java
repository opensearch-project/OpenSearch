/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RecipientTypeRegistryTests extends OpenSearchTestCase {

    public void testFromValue() {
        RecipientTypeRegistry.registerRecipientType("ble1", new RecipientType("ble1"));
        RecipientTypeRegistry.registerRecipientType("ble2", new RecipientType("ble2"));

        // Valid Value
        RecipientType type = RecipientTypeRegistry.fromValue("ble1");
        assertNotNull(type);
        assertEquals("ble1", type.getType());

        // Invalid Value
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> RecipientTypeRegistry.fromValue("bleble"));
        MatcherAssert.assertThat("Unknown RecipientType: bleble. Must be 1 of these: [ble1, ble2]", is(equalTo(exception.getMessage())));
    }
}
