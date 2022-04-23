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
import org.gradle.testkit.runner.BuildResult;
import org.gradle.api.publish.maven.MavenPublication;

public class ZipPublishTests extends GradleUnitTestCase {

    @Test
    public void testZipPublish() throws IOException {
        System.out.println("Testing ZipPublishTests");
        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply(ZipPublish.class);
        assertTrue(project.getPluginManager().hasPlugin("opensearch.zippublish"));
        assertTrue(dependencies.contains(project.getTasks().findByName("publishMavenzipPublicationToZipstagingRepository")));
    }


}
