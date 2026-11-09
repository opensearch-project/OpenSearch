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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.monitor.jvm;

import org.apache.lucene.util.Constants;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

public class JvmInfoTests extends OpenSearchTestCase {

    public void testUseG1GC() {
        // if we are running on HotSpot, and the test JVM was started
        // with UseG1GC, then JvmInfo should successfully report that
        // G1GC is enabled
        if (Constants.JVM_NAME.contains("HotSpot") || Constants.JVM_NAME.contains("OpenJDK")) {
            assertEquals(Boolean.toString(isG1GCEnabled()), JvmInfo.jvmInfo().useG1GC());
        } else {
            assertEquals("unknown", JvmInfo.jvmInfo().useG1GC());
        }
    }

    private boolean isG1GCEnabled() {
        final String argline = System.getProperty("tests.jvm.argline");
        final boolean g1GCEnabled = flagIsEnabled(argline, "UseG1GC");
        // for JDK 9 the default collector when no collector is specified is G1 GC
        final boolean noOtherCollectorSpecified = argline == null
            || (!flagIsEnabled(argline, "UseParNewGC")
                && !flagIsEnabled(argline, "UseParallelGC")
                && !flagIsEnabled(argline, "UseParallelOldGC")
                && !flagIsEnabled(argline, "UseSerialGC")
                && !flagIsEnabled(argline, "UseConcMarkSweepGC"));
        return g1GCEnabled || noOtherCollectorSpecified;
    }

    private boolean flagIsEnabled(String argline, String flag) {
        final boolean containsPositiveFlag = argline != null && argline.contains("-XX:+" + flag);
        if (!containsPositiveFlag) return false;
        final int index = argline.lastIndexOf(flag);
        return argline.charAt(index - 1) == '+';
    }

    public void testSecretArgumentsAreMasked() {
        assertEquals(
            "-Djavax.net.ssl.trustStorePassword=" + JvmInfo.SECRET_MASK,
            JvmInfo.maskSecretArguments(new String[] { "-Djavax.net.ssl.trustStorePassword=example-secret" })[0]
        );
    }

    public void testEverySecretKeyPatternIsMasked() {
        final String[] secretKeys = {
            "some.password",
            "some.secret",
            "some.credential",
            "some.authToken",
            "some.accessToken",
            "some.sessionToken",
            "aws.accessKey",
            "some.privateKey",
            "javax.net.ssl.keyStorePassword",
            "some.keyPass",
            "some.storePass" };
        for (final String key : secretKeys) {
            final String masked = JvmInfo.maskSecretArguments(new String[] { "-D" + key + "=example-secret" })[0];
            assertEquals("key [" + key + "] should be masked", "-D" + key + "=" + JvmInfo.SECRET_MASK, masked);
            assertFalse("key [" + key + "] leaked its value", masked.contains("example-secret"));
        }
    }

    public void testSecretKeyMatchingIsCaseInsensitive() {
        assertEquals("-Dmy.PASSWORD=" + JvmInfo.SECRET_MASK, JvmInfo.maskSecretArguments(new String[] { "-Dmy.PASSWORD=example" })[0]);
    }

    public void testNonSecretArgumentsArePreserved() {
        final String[] arguments = {
            "-Xmx4g",
            "-XX:+UseG1GC",
            "-Dfoo=bar",
            "-Djavax.net.ssl.trustStore=/path/to/truststore",
            "-Dmy.token.count=5",
            "-Dmax.tokens=512",
            "--add-opens=java.base/java.nio=ALL-UNNAMED" };
        assertArrayEquals(arguments, JvmInfo.maskSecretArguments(arguments));
    }

    public void testArgumentsWithoutAValueArePreserved() {
        final String[] arguments = { "-Dsome.password", "-Dfoo", "-Xshare:auto" };
        assertArrayEquals(arguments, JvmInfo.maskSecretArguments(arguments));
    }

    public void testEmptySecretValueIsStillMasked() {
        assertEquals("-Dfoo.password=" + JvmInfo.SECRET_MASK, JvmInfo.maskSecretArguments(new String[] { "-Dfoo.password=" })[0]);
    }

    public void testMaskDoesNotDiscloseSecretLength() {
        final String shortSecret = JvmInfo.maskSecretArguments(new String[] { "-Dfoo.password=a" })[0];
        final String longSecret = JvmInfo.maskSecretArguments(new String[] { "-Dfoo.password=aVeryMuchLongerSecretValue" })[0];
        assertEquals(shortSecret, longSecret);
    }

    public void testMaskingDoesNotMutateTheSourceArray() {
        final String[] arguments = { "-Dfoo.password=example-secret" };
        JvmInfo.maskSecretArguments(arguments);
        assertEquals("-Dfoo.password=example-secret", arguments[0]);
    }

    public void testMaskingHandlesAnEmptyArgumentList() {
        assertArrayEquals(new String[0], JvmInfo.maskSecretArguments(new String[0]));
    }

    public void testReportedArgumentsDoNotExposeSecrets() {
        for (final String argument : JvmInfo.jvmInfo().getInputArguments()) {
            if (argument != null && argument.startsWith("-D") && argument.indexOf('=') > 0) {
                final String key = argument.substring("-D".length(), argument.indexOf('='));
                if (key.toLowerCase(Locale.ROOT).contains("password")) {
                    assertTrue("argument [" + argument + "] should be masked", argument.endsWith("=" + JvmInfo.SECRET_MASK));
                }
            }
        }
    }
}
