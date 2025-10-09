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
import org.gradle.api.internal.artifacts.repositories.DefaultIvyArtifactRepository;
import org.gradle.api.internal.artifacts.repositories.descriptor.IvyRepositoryDescriptor;
import org.gradle.api.internal.artifacts.repositories.layout.DefaultIvyPatternRepositoryLayout;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

public class JdkDownloadPluginSetupRepositoryTests extends GradleUnitTestCase {

    private final Project project = ProjectBuilder.builder().build();

    @Before
    public void setup() {
        project.getPlugins().apply("opensearch.jdk-download");
    }

    @Test
    public void shouldCreateRepositoryForAdoptiumJdk8() throws NoSuchFieldException, IllegalAccessException {
        // given
        Jdk jdk = createJdk("adoptium", "8u302-b08", "linux", "aarch64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_adoptium_8u302-b08");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://github.com/adoptium/temurin8-binaries/releases/download/"));
        assertThat(getPattern(target), is("jdk8u302-b08/OpenJDK8U-jdk_[classifier]_[module]_hotspot_8u302b08.[ext]"));
    }

    @Test
    public void shouldCreateRepositoryForAdoptiumJdk17GaRelease() throws NoSuchFieldException, IllegalAccessException {
        // given - GA release (no dot in version)
        Jdk jdk = createJdk("adoptium", "17+35", "windows", "x64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_adoptium_17+35");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://github.com/adoptium/temurin17-binaries/releases/download/"));
        assertThat(getPattern(target), is("jdk-17+35/OpenJDK17-jdk_[classifier]_[module]_hotspot_17_35.[ext]"));
    }

    @Test
    public void shouldCreateRepositoryForAdoptiumJdk17UpdateRelease() throws NoSuchFieldException, IllegalAccessException {
        // given - Update release (with dot in version)
        Jdk jdk = createJdk("adoptium", "17.0.1+12", "darwin", "x64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_adoptium_17.0.1+12");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://github.com/adoptium/temurin17-binaries/releases/download/"));
        assertThat(getPattern(target), is("jdk-17.0.1+12/OpenJDK17U-jdk_[classifier]_[module]_hotspot_17.0.1_12.[ext]"));
    }

    @Test
    public void shouldCreateRepositoryForAdoptiumJdk21() throws NoSuchFieldException, IllegalAccessException {
        // given - JDK 20+ always has U suffix
        Jdk jdk = createJdk("adoptium", "21+35", "linux", "aarch64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_adoptium_21+35");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://github.com/adoptium/temurin21-binaries/releases/download/"));
        assertThat(getPattern(target), is("jdk-21+35/OpenJDK21U-jdk_[classifier]_[module]_hotspot_21_35.[ext]"));
    }

    @Test
    public void shouldCreateRepositoryForAdoptOpenJdkJdk8() throws NoSuchFieldException, IllegalAccessException {
        // given
        Jdk jdk = createJdk("adoptopenjdk", "8u302-b08", "linux", "x64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_adoptopenjdk_8u302-b08");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://api.adoptopenjdk.net/v3/binary/version/"));
        assertThat(getPattern(target), is("jdk8u302-b08/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk"));
    }

    @Test
    public void shouldCreateRepositoryForAdoptOpenJdkJdk11() throws NoSuchFieldException, IllegalAccessException {
        // given
        Jdk jdk = createJdk("adoptopenjdk", "11.0.11+9", "windows", "x64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_adoptopenjdk_11.0.11+9");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://api.adoptopenjdk.net/v3/binary/version/"));
        assertThat(getPattern(target), is("jdk-11.0.11+9/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk"));
    }

    @Test
    public void shouldCreateRepositoryForOpenJdkWithHash() throws NoSuchFieldException, IllegalAccessException {
        // given - hash is part of the version string
        Jdk jdk = createJdk("openjdk", "13.0.1+9@cec27d702aa74d5a8630c65ae61e4305", "darwin", "x64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_openjdk_13.0.1+9@cec27d702aa74d5a8630c65ae61e4305");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://download.oracle.com"));
        assertThat(
            getPattern(target),
            is("java/GA/jdk13.0.1/cec27d702aa74d5a8630c65ae61e4305/9/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]")
        );
    }

    @Test
    public void shouldCreateRepositoryForOpenJdkWithoutHash() throws NoSuchFieldException, IllegalAccessException {
        // given
        Jdk jdk = createJdk("openjdk", "12+33", "linux", "x64");

        // when
        setupRepository(jdk);

        // then
        DefaultIvyArtifactRepository target = findRepository("jdk_repo_openjdk_12+33");
        assertThat(target, is(instanceOf(DefaultIvyArtifactRepository.class)));
        assertThat(target.getUrl().toString(), equalTo("https://download.oracle.com"));
        assertThat(getPattern(target), is("java/GA/jdk12/33/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]"));
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

    private void setupRepository(Jdk jdk) {
        JdkDownloadPlugin plugin = (JdkDownloadPlugin) project.getPlugins().getPlugin("opensearch.jdk-download");
        plugin.setupRepository(project, jdk);
    }

    private DefaultIvyArtifactRepository findRepository(String name) {
        return (DefaultIvyArtifactRepository) project.getRepositories().findByName(name);
    }

    private String getPattern(DefaultIvyArtifactRepository input) throws NoSuchFieldException, IllegalAccessException {
        DefaultIvyPatternRepositoryLayout layout = (DefaultIvyPatternRepositoryLayout) input.getRepositoryLayout();
        Set<String> patterns = new HashSet<>();
        layout.apply(URI.create("test"), new IvyRepositoryDescriptor.Builder("test", URI.create("test")) {
            @Override
            public void addArtifactPattern(String declaredPattern) {
                patterns.add(declaredPattern);
            }

            @Override
            public void addArtifactResource(URI rootUri, String pattern) {}

            @Override
            public void addIvyPattern(String declaredPattern) {}

            @Override
            public void addIvyResource(URI baseUri, String pattern) {}
        });
        assertThat(patterns.size(), is(1));
        return patterns.stream().findFirst().get();
    }
}
