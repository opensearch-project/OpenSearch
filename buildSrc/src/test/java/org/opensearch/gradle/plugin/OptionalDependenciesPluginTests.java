/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.plugin;

import org.apache.maven.model.Dependency;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;
import org.opensearch.gradle.test.GradleUnitTestCase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static org.gradle.testkit.runner.TaskOutcome.SUCCESS;
import static org.hamcrest.CoreMatchers.is;

public class OptionalDependenciesPluginTests extends GradleUnitTestCase {
    private TemporaryFolder projectDir;

    @Before
    public void setUp() throws Exception {
        projectDir = new TemporaryFolder();
        projectDir.create();
    }

    @After
    public void tearDown() {
        projectDir.delete();
    }

    public void testApply() throws FileNotFoundException, IOException, XmlPullParserException {
        final File mavenRepoDir = new File(projectDir.getRoot(), "mavenrepo");

        try (InputStream in = getClass().getClassLoader().getResourceAsStream("plugin/optional-dependencies.gradle")) {
            try (OutputStream out = new FileOutputStream(projectDir.newFile("build.gradle"))) {
                in.transferTo(out);
            }
        }

        GradleRunner runner = GradleRunner.create()
            .forwardOutput()
            .withPluginClasspath()
            .withArguments("publish", "-PrepoUrl=" + mavenRepoDir.toURI().toURL())
            .withProjectDir(projectDir.getRoot());

        BuildResult result = runner.build();
        assertEquals(SUCCESS, result.task(":" + "publish").getOutcome());

        final String name = projectDir.getRoot().getName();
        assertDependency(mavenRepoDir, name);
    }

    private void assertDependency(File repoUrl, String name) throws FileNotFoundException, IOException, XmlPullParserException {
        final File pom = new File(repoUrl, "org/custom/group/" + name + "/1.0.0/" + name + "-1.0.0.pom");
        assertThat(pom.exists(), is(true));

        final MavenXpp3Reader reader = new MavenXpp3Reader();
        final Model model = reader.read(new FileReader(pom));

        final Optional<Dependency> dependecyOpt = model.getDependencies()
            .stream()
            .filter(d -> d.getArtifactId().equals("commons-lang3"))
            .findAny();

        assertThat(dependecyOpt.isPresent(), is(true));
        assertThat(dependecyOpt.get().isOptional(), is(true));
    }
}
