/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.fips.truststore;

import org.opensearch.test.OpenSearchTestCase;

import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.stream.IntStream;

public class SecureRandomHolderTests extends OpenSearchTestCase {

    public void testSecureRandom() {
        assumeTrue("Should only run when BCFIPS provider is installed.", inFipsJvm());

        var secureRandom = SecureRandomHolder.getSecureRandom();
        assertNotNull(secureRandom);
        assertTrue(secureRandom.getProvider().getName().startsWith("BCFIPS"));

        var algorithm = secureRandom.getAlgorithm();
        var algorithmUpper = algorithm.toUpperCase(Locale.ROOT);

        assertTrue(
            "SecureRandom algorithm should be SHA512 HMAC DRBG based, but was: " + algorithm,
            algorithmUpper.contains("SHA512") || algorithmUpper.contains("HMAC") || algorithmUpper.contains("DRBG")
        );

        var randomBytes = new byte[32];
        secureRandom.nextBytes(randomBytes);
        var hasNonZero = IntStream.range(0, randomBytes.length).anyMatch(i -> randomBytes[i] != 0);
        assertTrue("SecureRandom should generate non-zero random bytes (proper entropy)", hasNonZero);
    }

    public void testSecureRandomWithoutBcfips() {
        assumeFalse("Should only run when BCFIPS provider is NOT installed.", inFipsJvm());

        // When BCFIPS is not available, accessing SecureRandomHolder throws ExceptionInInitializerError
        // because the static RANDOM field initialization fails
        var exception = assertThrows(ExceptionInInitializerError.class, SecureRandomHolder::getSecureRandom);

        var originalCause = exception.getCause().getCause();
        assertTrue(originalCause instanceof GeneralSecurityException);
        assertEquals("no such provider: BCFIPS", originalCause.getMessage());
    }
}
