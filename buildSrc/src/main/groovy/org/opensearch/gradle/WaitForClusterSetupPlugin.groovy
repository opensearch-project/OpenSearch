/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project

class WaitForClusterSetupPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.extensions.create('clusterSetup', ClusterSetupExtension)
        
        project.task('waitForClusterSetup', type: WaitForClusterSetupTask)
    }
}

class ClusterSetupExtension {
    boolean securityEnabled = false
    String protocol = 'http'
    String username = 'admin'
    String password = 'admin'
    int timeoutMs = 180000
}