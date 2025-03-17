/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.launchers;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.junit.After;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;

public class SystemJvmOptionsTests extends LaunchersTestCase {

    private static final String FILE_NAME = CryptoServicesRegistrar.isInApprovedOnlyMode() ? "fips_java.security" : "java.security";
    private static final int MIN_RUNTIME_VERSION = 11;
    private static final int MAX_RUNTIME_VERSION = 21;

    private Path tempFile;

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(tempFile);
    }

    public void testSetJavaSecurityProperties() throws Exception {
        createSecurityFile(FILE_NAME);
        var jvmOptions = SystemJvmOptions.systemJvmOptions(globalTempDir(), Runtime.version());
        assertThat(jvmOptions, hasItem("-Djava.security.properties=" + globalTempDir().toAbsolutePath() + "/" + FILE_NAME));
    }

    public void testFailSetJavaSecurityProperties() throws Exception {
        createSecurityFile("unknown.security");
        assertThrows(
            FileNotFoundException.class,
            () -> SystemJvmOptions.systemJvmOptions(globalTempDir().toAbsolutePath(), Runtime.version())
        );
    }

    public void testFipsOption() throws Exception {
        createSecurityFile(FILE_NAME);
        var jvmOptions = SystemJvmOptions.systemJvmOptions(globalTempDir().toAbsolutePath(), Runtime.version());
        var fipsProperty = "-Dorg.bouncycastle.fips.approved_only=true";
        if (CryptoServicesRegistrar.isInApprovedOnlyMode()) {
            assertThat(jvmOptions, hasItem(fipsProperty));
        } else {
            assertThat(jvmOptions, not(hasItem(fipsProperty)));
        }
    }

    public void testSecurityManagerOption() throws Exception {
        createSecurityFile(FILE_NAME);
        var runtimeVersion = Runtime.Version.parse(String.valueOf(randomIntBetween(MIN_RUNTIME_VERSION, 17)));
        var jvmOptions = SystemJvmOptions.systemJvmOptions(globalTempDir().toAbsolutePath(), runtimeVersion);
        assertThat(jvmOptions, not(hasItem("-Djava.security.manager=allow")));

        runtimeVersion = Runtime.Version.parse(String.valueOf(randomIntBetween(18, MAX_RUNTIME_VERSION)));
        jvmOptions = SystemJvmOptions.systemJvmOptions(globalTempDir().toAbsolutePath(), runtimeVersion);
        assertThat(jvmOptions, hasItem("-Djava.security.manager=allow"));
    }

    public void testShowCodeDetailsOption() throws Exception {
        createSecurityFile(FILE_NAME);
        var runtimeVersion = Runtime.Version.parse(String.valueOf(randomIntBetween(MIN_RUNTIME_VERSION, 13)));
        var jvmOptions = SystemJvmOptions.systemJvmOptions(globalTempDir().toAbsolutePath(), runtimeVersion);
        assertThat(jvmOptions, not(hasItem("-XX:+ShowCodeDetailsInExceptionMessages")));

        runtimeVersion = Runtime.Version.parse(String.valueOf(randomIntBetween(14, MAX_RUNTIME_VERSION)));
        jvmOptions = SystemJvmOptions.systemJvmOptions(globalTempDir().toAbsolutePath(), runtimeVersion);
        assertThat(jvmOptions, hasItem("-XX:+ShowCodeDetailsInExceptionMessages"));
    }

    private void createSecurityFile(String fileName) throws Exception {
        tempFile = Files.createFile(globalTempDir().resolve(fileName));
    }
}
