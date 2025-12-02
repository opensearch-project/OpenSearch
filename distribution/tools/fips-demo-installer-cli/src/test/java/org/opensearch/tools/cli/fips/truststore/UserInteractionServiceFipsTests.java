/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

public class UserInteractionServiceFipsTests extends UserInteractionServiceTests {

    public void testGenerateSecurePassword() {
        var service = UserInteractionService.getInstance();
        var password = service.generateSecurePassword();

        assertEquals(24, password.length()); // default password length
        assertTrue(password.chars().allMatch(c -> Character.isLetterOrDigit(c) || "!@#$%^&*".indexOf(c) >= 0));
    }

}
