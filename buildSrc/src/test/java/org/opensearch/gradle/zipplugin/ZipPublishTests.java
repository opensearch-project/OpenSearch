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

public class ZipPublishTests extends GradleUnitTestCase {

    @Test
    public void testZipPublish() throws IOException {
        System.out.println("Testing ZipPublishTests");
        Project project = ProjectBuilder.builder().build();
        project.getPluginManager().apply(ZipPublish.class);
        assertTrue(project.getPluginManager().hasPlugin("opensearch.zippublish"));
        Task zipPublishTask = project.getTasks().findByName("publishMavenzipPublicationToZipstagingRepository");
        assertNotNull(zipPublishTask);
    }


}
