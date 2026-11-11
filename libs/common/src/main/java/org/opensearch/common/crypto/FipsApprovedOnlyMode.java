/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

/**
 * Tells whether this JVM runs BouncyCastle FIPS in approved-only mode, which is what FIPS mode means at runtime.
 * <p>
 * The answer comes from BouncyCastle itself rather than from an environment variable or from a registered provider: the launcher
 * may declare the BCFIPS provider without FIPS mode, and only the JVM knows whether the FIPS settings were actually applied.
 * BouncyCastle is accessed through reflection because it is not on every module's compile classpath.
 *
 * @opensearch.internal
 */
public final class FipsApprovedOnlyMode {

    private FipsApprovedOnlyMode() {}

    /**
     * @return true if BouncyCastle FIPS is on the classpath and runs in approved-only mode
     */
    public static boolean isActive() {
        try {
            // Equivalent to: CryptoServicesRegistrar.isInApprovedOnlyMode()
            final Class<?> registrarClass = Class.forName("org.bouncycastle.crypto.CryptoServicesRegistrar");
            final Object approvedOnly = registrarClass.getMethod("isInApprovedOnlyMode").invoke(null);
            return Boolean.TRUE.equals(approvedOnly);
        } catch (ReflectiveOperationException | LinkageError e) {
            return false;
        }
    }
}
