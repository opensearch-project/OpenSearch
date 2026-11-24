/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.info;

import java.util.function.Function;
import java.util.function.Supplier;

public class FipsBuildParams {

    @Deprecated
    public static final String FIPS_BUILD_PARAM_FOR_TESTS = "tests.fips.enabled";
    public static final String FIPS_BUILD_PARAM = "crypto.standard";
    public static final String DEFAULT_FIPS_MODE = "FIPS-140-3";

    private static String fipsMode;
    static Supplier<String> fipsModeEnvSupplier = () -> System.getenv("OPENSEARCH_FIPS_MODE");

    public static void init(Function<String, Object> fipsValue) {
        var fipsBuildParamForTests = Boolean.parseBoolean((String) fipsValue.apply(FIPS_BUILD_PARAM_FOR_TESTS));
        var fipsBuildParam = (String) fipsValue.apply(FIPS_BUILD_PARAM);

        if (fipsBuildParamForTests || DEFAULT_FIPS_MODE.equals(fipsBuildParam)) {
            fipsMode = DEFAULT_FIPS_MODE;
        } else {
            fipsMode = "any-supported";
        }
    }

    private FipsBuildParams() {}

    /**
     * Whether the build targets {@code crypto.standard=FIPS-140-3}: the artifacts and test classpaths contain the
     * BouncyCastle FIPS providers. This alone does not put any JVM into FIPS mode; see {@link #isFipsModeEnabled()}.
     */
    public static boolean isFipsBuild() {
        return DEFAULT_FIPS_MODE.equals(fipsMode);
    }

    /**
     * Whether JVMs started by the build run in FIPS mode: a {@linkplain #isFipsBuild() FIPS build} with
     * {@code OPENSEARCH_FIPS_MODE=true}, the same variable that turns FIPS mode on in a distribution. Test JVMs then use
     * only the providers of {@code fips_java.security}, in approved-only mode, and the {@code *FipsTests} variants run.
     * <p>
     * This is a build-time decision. It does not tell whether a running JVM is in approved-only mode; that is decided by the
     * JVM's own java.security and BouncyCastle settings.
     */
    public static boolean isFipsModeEnabled() {
        return isFipsBuild() && "true".equalsIgnoreCase(fipsModeEnvSupplier.get());
    }

    /**
     * @deprecated use {@link #isFipsBuild()}; the name suggested a JVM in FIPS mode.
     */
    @Deprecated
    public static boolean isInFipsMode() {
        return isFipsBuild();
    }

    /**
     * @deprecated use {@link #isFipsModeEnabled()}.
     */
    @Deprecated
    public static boolean isInFipsApprovedOnlyMode() {
        return isFipsModeEnabled();
    }

    public static String getFipsMode() {
        return fipsMode;
    }

}
