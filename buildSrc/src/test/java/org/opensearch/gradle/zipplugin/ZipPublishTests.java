/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.zipplugin;

import org.gradle.testfixtures.ProjectBuilder;
import org.gradle.api.Project;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.io.FileWriter;
import java.nio.file.Files;
import org.gradle.testkit.runner.GradleRunner;
import org.gradle.testkit.runner.BuildResult;

public class ZipPublishTests extends GradleUnitTestCase {

    @Test
    public void testZipPublish() throws IOException {
        System.out.println("Testing ZipPublishTests");
        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply("opensearch.zippublish");
        assertTrue(project.getPluginManager().hasPlugin("opensearch.zippublish"));
        assertTrue(project.getTasks().findByName("publishMavenzipPublicationToZipstagingRepository"));
        //assertTrue("plugin should have a task", project.getTasks().findByName("publishMavenzipPublicationToZipstagingRepository"));
        File projectDir = new File("build/functionalTest");
        Files.createDirectories(projectDir.toPath());
        writeString(new File(projectDir, "settings.gradle"), "");
        writeString(new File(projectDir, "build.gradle"), "plugins {" + "  id('opensearch.zippublish')" + "}");

        GradleRunner runner = GradleRunner.create();
        runner.forwardOutput();
        runner.withPluginClasspath();
        runner.withArguments("publishMavenzipPublicationToZipstagingRepository");
        runner.withProjectDir(projectDir);
        BuildResult result = runner.build();

        assertTrue(result.getOutput().contains("someoutput from the publishMavenzipPublicationToZipstagingRepository task"));
    }

    private void writeString(File file, String string) throws IOException {
        try (Writer writer = new FileWriter(file)) {
            writer.write(string);
        }
    }

}
