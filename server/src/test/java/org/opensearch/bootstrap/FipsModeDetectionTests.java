/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.crypto.FipsApprovedOnlyMode;
import org.opensearch.test.OpenSearchTestCase;

import java.security.Security;
import java.util.ArrayList;
import java.util.List;

public class FipsModeDetectionTests extends OpenSearchTestCase {

    public void testApprovedOnlyMode() {
        assertFalse(FipsApprovedOnlyMode.isActive());
    }

    public void testFipsModeRequestedAndActive() {
        assertTrue(Bootstrap.isFipsModeRequested("true", true));
        assertTrue(Bootstrap.isFipsModeRequested("TRUE", true));
    }

    public void testApprovedOnlyModeWithoutTheVariableIsLeftAlone() {
        // FIPS configured at the JDK or OS level, without asking OpenSearch to enforce its FIPS rules
        assertFalse(Bootstrap.isFipsModeRequested(null, true));
        assertFalse(Bootstrap.isFipsModeRequested("false", true));
    }

    public void testNeitherRequestedNorActive() {
        assertFalse(Bootstrap.isFipsModeRequested(null, false));
        assertFalse(Bootstrap.isFipsModeRequested("false", false));
    }

    public void testFipsModeRequestedButNotActiveFails() {
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> Bootstrap.isFipsModeRequested("true", false));

        assertTrue(e.getMessage().contains("does not run in approved-only mode"));
    }

    public void testJavaSecurityFileReplacesTheJdkFile() {
        // "-Djava.security.properties==<file>" arrives as the value "=<file>": the JVM loaded only that file, not the JDK's
        assertTrue(System.getProperty("java.security.properties").startsWith("="));
        assertFalse(declaredProviders().isEmpty());
    }

    /**
     * The provider entries this JVM loaded from its java.security file, in slot order: a provider name or class name each.
     * Read through {@link Security#getProperty} because tests may not open the file itself.
     */
    static List<String> declaredProviders() {
        final List<String> providers = new ArrayList<>();
        for (int slot = 1; Security.getProperty("security.provider." + slot) != null; slot++) {
            final String entry = Security.getProperty("security.provider." + slot).trim();
            providers.add(entry.split("\\s+")[0]);
        }
        return providers;
    }
}
