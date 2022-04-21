/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
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