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
import org.gradle.api.Plugin;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.junit.Test;
import java.io.IOException;
import org.gradle.api.Task;
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository;

public class ZipPublishTests extends GradleUnitTestCase {

    @Test
    public void testZipPublish() throws IOException {
        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply("opensearch.zippublish");
        assertTrue(project.getPluginManager().hasPlugin("opensearch.zippublish"));
        assertNotNull(
            "plugin extension created with the right name",
            project.getExtensions().findByName(ZipPublish.EXTENSION_NAME)
        );
        assertNotNull("plugin extensions has the right type", project.getExtensions().findByType(ZipPublishExtension.class));
        
        assertNotNull(project.getTasks().findByType(PublishToMavenRepository.class));
        
        //project.tasks["publishTestPublicationToMavenRepository"] instanceof PublishToMavenRepository

        //assertNotNull(project.getTasks().getByName("hello"));
    }


}
