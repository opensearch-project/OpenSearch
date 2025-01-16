/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import org.opensearch.gradle.info.FipsBuildParams;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

public class SecureRandomProvider {

    private final static MethodHandle MH_BC_SECURE_RANDOM;

    static {
        MethodHandle mh = null;
        if (FipsBuildParams.isInFipsMode()) {
            try {
                final Class<?> cryptoServicesRegistrarClass = Class.forName("org.bouncycastle.crypto.CryptoServicesRegistrar");
                mh = MethodHandles.publicLookup()
                    .findStatic(cryptoServicesRegistrarClass, "getSecureRandom", MethodType.methodType(SecureRandom.class));
            } catch (final Throwable ex) {
                throw new SecurityException("Unable to find org.bouncycastle.crypto.CryptoServicesRegistrar instance", ex);
            }
        }
        MH_BC_SECURE_RANDOM = mh;
    }

    public static SecureRandom getSecureRandom() throws GeneralSecurityException {
        if (MH_BC_SECURE_RANDOM == null) {
            return new SecureRandom();
        } else try {
            return (SecureRandom) MH_BC_SECURE_RANDOM.invoke();
        } catch (final Throwable ex) {
            throw new GeneralSecurityException(ex);
        }
    }
}
