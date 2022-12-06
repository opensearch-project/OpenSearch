/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import org.opensearch.gradle.test.GradleUnitTestCase;

public class ArchitectureTests extends GradleUnitTestCase {

    final String architecture = System.getProperty("os.arch", "");

    public void testCurrentArchitecture() {
        assertEquals(Architecture.X64, currentArchitecture("amd64"));
        assertEquals(Architecture.X64, currentArchitecture("x86_64"));
        assertEquals(Architecture.ARM64, currentArchitecture("aarch64"));
        assertEquals(Architecture.S390X, currentArchitecture("s390x"));
        assertEquals(Architecture.PPC64LE, currentArchitecture("ppc64le"));
    }

    public void testInvalidCurrentArchitecture() {
        assertThrows("can not determine architecture from [", IllegalArgumentException.class, () -> currentArchitecture("fooBar64"));
    }

    /**
     * Determines the return value of {@link Architecture#current()} based on a string representing a potential OS Architecture.
     *
     * @param osArchToTest  An expected value of the {@code os.arch} system property on another architecture.
     * @return the value of the {@link Architecture} enum which would have resulted with the given value.
     * @throws IllegalArgumentException if the string is not mapped to a value of the {@link Architecture} enum.
     */
    private Architecture currentArchitecture(String osArchToTest) throws IllegalArgumentException {
        // Test new architecture
        System.setProperty("os.arch", osArchToTest);
        try {
            return Architecture.current();
        } finally {
            // Restore actual architecture property value
            System.setProperty("os.arch", this.architecture);
        }
    }
}
