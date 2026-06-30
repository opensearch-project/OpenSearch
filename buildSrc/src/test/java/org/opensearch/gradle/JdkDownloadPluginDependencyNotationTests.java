/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

public class JdkDownloadPluginDependencyNotationTests extends GradleUnitTestCase {

    private final Project project = ProjectBuilder.builder().build();

    @Before
    public void setup() {
        project.getPlugins().apply("opensearch.jdk-download");
    }

    @Test
    public void shouldUseMacPlatformForDarwinWithAdoptium() {
        // given
        Jdk jdk = createJdk("adoptium", "17.0.1+12", "darwin", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("adoptium_17:mac:17.0.1:x64@tar.gz"));
    }

    @Test
    public void shouldUseMacPlatformForDarwinWithAdoptOpenJdk() {
        // given
        Jdk jdk = createJdk("adoptopenjdk", "11.0.11+9", "darwin", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("adoptopenjdk_11:mac:11.0.11:x64@tar.gz"));
    }

    @Test
    public void shouldUseOsxPlatformForDarwinWithOpenjdk() {
        // given
        Jdk jdk = createJdk("openjdk", "13.0.1+9@cec27d702aa74d5a8630c65ae61e4305", "darwin", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("openjdk_13:osx:13.0.1:x64@tar.gz"));
    }

    @Test
    public void shouldUseMacPlatformForMacWithAdoptium() {
        // given
        Jdk jdk = createJdk("adoptium", "21+35", "mac", "aarch64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("adoptium_21:mac:21:aarch64@tar.gz"));
    }

    @Test
    public void shouldUseOsxPlatformForMacWithOpenjdk() {
        // given
        Jdk jdk = createJdk("openjdk", "12+33", "mac", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("openjdk_12:osx:12:x64@tar.gz"));
    }

    @Test
    public void shouldUseLinuxPlatformAsIs() {
        // given
        Jdk jdk = createJdk("adoptium", "8u302-b08", "linux", "aarch64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("adoptium_8:linux:8u302:aarch64@tar.gz"));
    }

    @Test
    public void shouldUseWindowsPlatformAsIsWithZipExtension() {
        // given
        Jdk jdk = createJdk("adoptium", "17+35", "windows", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("adoptium_17:windows:17:x64@zip"));
    }

    @Test
    public void shouldUseTarGzExtensionForNonWindowsPlatforms() {
        // given
        Jdk jdk = createJdk("adoptopenjdk", "8u302-b08", "linux", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then
        assertThat(target, equalTo("adoptopenjdk_8:linux:8u302:x64@tar.gz"));
    }

    @Test
    public void shouldIncludeArchitectureInNotation() {
        // given - x64 architecture
        Jdk jdkX64 = createJdk("adoptium", "17.0.1+12", "linux", "x64");

        // when
        String targetX64 = JdkDownloadPlugin.dependencyNotation(jdkX64);

        // then
        assertThat(targetX64, equalTo("adoptium_17:linux:17.0.1:x64@tar.gz"));

        // given - aarch64 architecture
        Jdk jdkAarch64 = createJdk("adoptium", "17.0.1+12", "linux", "aarch64");

        // when
        String targetAarch64 = JdkDownloadPlugin.dependencyNotation(jdkAarch64);

        // then
        assertThat(targetAarch64, equalTo("adoptium_17:linux:17.0.1:aarch64@tar.gz"));
    }

    @Test
    public void shouldFormatCompleteNotationCorrectly() {
        // given
        Jdk jdk = createJdk("adoptium", "21.0.2+13", "windows", "x64");

        // when
        String target = JdkDownloadPlugin.dependencyNotation(jdk);

        // then - format: groupName:platform:baseVersion:architecture@extension
        assertThat(target, equalTo("adoptium_21:windows:21.0.2:x64@zip"));
    }

    private Jdk createJdk(String vendor, String version, String platform, String architecture) {
        Jdk jdk = new Jdk("test", project.getConfigurations().create("test_" + System.nanoTime()), project.getObjects());
        jdk.setVendor(vendor);
        jdk.setVersion(version);
        jdk.setPlatform(platform);
        jdk.setArchitecture(architecture);
        jdk.finalizeValues();
        return jdk;
    }
}
