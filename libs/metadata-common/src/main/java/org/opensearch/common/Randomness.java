/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.lang.reflect.Method;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Provides factory methods for producing reproducible sources of
 * randomness. Reproducible sources of randomness contribute to
 * reproducible tests. When running the OpenSearch test suite, the
 * test runner will establish a global random seed accessible via the
 * system property "tests.seed". By seeding a random number generator
 * with this global seed, we ensure that instances of Random produced
 * with this class produce reproducible sources of randomness under
 * when running under the OpenSearch test suite. Alternatively,
 * a reproducible source of randomness can be produced by providing a
 * setting a reproducible seed. When running the OpenSearch server
 * process, non-reproducible sources of randomness are provided (unless
 * a setting is provided for a module that exposes a seed setting (e.g.,
 * NodeEnvironment#NODE_ID_SEED_SETTING)).
 *
 * @opensearch.internal
 */
public final class Randomness {
    private static final Method currentMethod;
    private static final Method getRandomMethod;

    static {
        Method maybeCurrentMethod;
        Method maybeGetRandomMethod;
        try {
            Class<?> clazz = Class.forName("com.carrotsearch.randomizedtesting.RandomizedContext");
            maybeCurrentMethod = clazz.getMethod("current");
            maybeGetRandomMethod = clazz.getMethod("getRandom");
        } catch (Exception e) {
            maybeCurrentMethod = null;
            maybeGetRandomMethod = null;
        }
        currentMethod = maybeCurrentMethod;
        getRandomMethod = maybeGetRandomMethod;
    }

    private Randomness() {}

    /**
     * Provides a reproducible source of randomness seeded by a long
     * seed in the settings with the key setting.
     *
     * @param settings the settings containing the seed
     * @param setting  the setting to access the seed
     * @return a reproducible source of randomness
     */
    public static Random get(Settings settings, Setting<Long> setting) {
        if (setting.exists(settings)) {
            return new Random(setting.get(settings));
        } else {
            return get();
        }
    }

    /**
     * Provides a source of randomness that is reproducible when
     * running under the OpenSearch test suite, and otherwise
     * produces a non-reproducible source of randomness. Reproducible
     * sources of randomness are created when the system property
     * "tests.seed" is set and the security policy allows reading this
     * system property. Otherwise, non-reproducible sources of
     * randomness are created.
     *
     * @return a source of randomness
     * @throws IllegalStateException if running tests but was not able
     *                               to acquire an instance of Random from
     *                               RandomizedContext or tests are
     *                               running but tests.seed is not set
     */
    public static Random get() {
        if (currentMethod != null && getRandomMethod != null) {
            try {
                Object randomizedContext = currentMethod.invoke(null);
                return (Random) getRandomMethod.invoke(randomizedContext);
            } catch (ReflectiveOperationException e) {
                // unexpected, bail
                throw new IllegalStateException("running tests but failed to invoke RandomizedContext#getRandom", e);
            }
        } else {
            return getWithoutSeed();
        }
    }

    /**
     * Returns a {@link SecureRandom} via
     * {@code
     * FipsDRBG.SHA512_HMAC.fromEntropySource(new BasicEntropySourceProvider(entropySource, true)).build(null, true)
     * }
     * if BCFIPS is on classpath and the application is running in FIPS JVM,
     * otherwise it returns a non-approved {@link SecureRandom}.
     */
    public static SecureRandom createSecure() {
        try {
            // Equivalent to: boolean approvedOnly = CryptoServicesRegistrar.isInApprovedOnlyMode()
            var registrarClass = Class.forName("org.bouncycastle.crypto.CryptoServicesRegistrar");
            var isApprovedOnlyMethod = registrarClass.getMethod("isInApprovedOnlyMode");
            var approvedOnly = (Boolean) isApprovedOnlyMethod.invoke(null);

            if (approvedOnly) {
                var isPredictionResistant = true;
                var entropySource = SecureRandom.getInstance("DEFAULT", "BCFIPS");

                // Equivalent to:
                // EntropySourceProvider entropyProvider = new BasicEntropySourceProvider(entropySource, isPredictionResistant)
                var basicEntropyProviderClass = Class.forName("org.bouncycastle.crypto.util.BasicEntropySourceProvider");
                var entropyConstructor = basicEntropyProviderClass.getConstructor(SecureRandom.class, boolean.class);
                var entropyProvider = entropyConstructor.newInstance(entropySource, isPredictionResistant);

                // Equivalent to: FipsDRBG.Base sha512Hmac = FipsDRBG.SHA512_HMAC
                var fipsDrbgClass = Class.forName("org.bouncycastle.crypto.fips.FipsDRBG");
                var sha512HmacField = fipsDrbgClass.getField("SHA512_HMAC");
                var sha512Hmac = sha512HmacField.get(null);

                // Equivalent to: FipsDRBG.Builder builder = sha512Hmac.fromEntropySource(entropyProvider)
                var entropyProviderClass = Class.forName("org.bouncycastle.crypto.EntropySourceProvider");
                var fromEntropySourceMethod = sha512Hmac.getClass().getMethod("fromEntropySource", entropyProviderClass);
                var builder = fromEntropySourceMethod.invoke(sha512Hmac, entropyProvider);

                // Equivalent to: SecureRandom drbg = builder.build(null, isPredictionResistant)
                var buildMethod = builder.getClass().getMethod("build", byte[].class, boolean.class);
                var drbgInstance = buildMethod.invoke(builder, null, isPredictionResistant);

                return (SecureRandom) drbgInstance;
            }

            return new SecureRandom();
        } catch (ReflectiveOperationException | GeneralSecurityException e) {
            try {
                return SecureRandom.getInstanceStrong();
            } catch (NoSuchAlgorithmException ex) {
                throw new SecurityException("Failed to instantiate SecureRandom: " + e.getMessage(), e);
            }
        }
    }

    @SuppressForbidden(reason = "ThreadLocalRandom is okay when not running tests")
    private static Random getWithoutSeed() {
        assert currentMethod == null && getRandomMethod == null : "running under tests but tried to create non-reproducible random";
        return ThreadLocalRandom.current();
    }

    public static void shuffle(List<?> list) {
        Collections.shuffle(list, get());
    }
}
