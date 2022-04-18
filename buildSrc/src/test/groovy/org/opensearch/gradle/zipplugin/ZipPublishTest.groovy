package org.opensearch.gradle.zipplugin;

import org.gradle.testfixtures.ProjectBuilder
import org.gradle.api.Project


class ZipPublishTest {
    public void zippublish_plugin_should_add_task_to_project() {
        Project project = ProjectBuilder.builder().build()
        project.getPlugins().apply 'opensearch.zippublish'

        assertTrue(project.tasks.publishMavenzipPublicationToZipstagingRepository instanceof ZipPublish)
    }
}