/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.common.crypto.FipsApprovedOnlyMode;

import javax.crypto.Cipher;
import javax.crypto.Mac;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.List;

/**
 * Checks that FIPS test JVMs replace the JDK's java.security, like the distribution's launcher does, instead of appending to it:
 * only the providers declared in the loaded file (those of fips_java.security) are registered, so nothing falls back to a JDK
 * provider.
 */
public class FipsModeDetectionFipsTests extends FipsModeDetectionTests {

    @Override
    public void testApprovedOnlyMode() {
        assertTrue(FipsApprovedOnlyMode.isActive());
    }

    public void testOnlyTheDeclaredProvidersAreRegistered() {
        final List<String> declared = declaredProviders();
        final Provider[] registered = Security.getProviders();

        assertEquals("registered providers " + List.of(registered), declared.size(), registered.length);
        for (int i = 0; i < registered.length; i++) {
            final String nameOrClass = declared.get(i);
            final boolean matches = nameOrClass.equals(registered[i].getName()) || nameOrClass.equals(registered[i].getClass().getName());
            assertTrue("slot " + (i + 1) + " declares [" + nameOrClass + "] but holds [" + registered[i].getName() + "]", matches);
        }
    }

    public void testAlgorithmsWithoutAFipsProviderAreUnavailable() {
        // SunJCE would serve both, if the JDK's providers were still registered
        expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance("Blowfish"));
        expectThrows(NoSuchAlgorithmException.class, () -> Mac.getInstance("HmacMD5"));
    }
}
