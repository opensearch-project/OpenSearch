/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle

import groovy.json.JsonSlurper
import org.ajoberstar.grgit.Grgit
import org.ajoberstar.grgit.operation.BranchListOp
import org.gradle.api.DefaultTask
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.internal.os.OperatingSystem

import java.nio.file.Paths

class CheckCompatibilityTask extends DefaultTask {

    static final String REPO_URL = 'https://raw.githubusercontent.com/opensearch-project/opensearch-plugins/main/plugins/.meta'

    @Input
    List repositoryUrls = project.hasProperty('repositoryUrls') ? project.property('repositoryUrls').split(',') : getRepoUrls()

    @Input
    String ref = project.hasProperty('ref') ? project.property('ref') : 'main'

    @Internal
    List failedComponents = []

    @Internal
    List gitFailedComponents = []

    @Internal
    List compatibleComponents = []

    @TaskAction
    void checkCompatibility() {
        repositoryUrls.parallelStream().forEach { repositoryUrl ->
            logger.lifecycle("Checking compatibility for: $repositoryUrl with ref: $ref")
            def tempDir = File.createTempDir()
            try {
                if (cloneAndCheckout(repositoryUrl, tempDir)) {
                    if (repositoryUrl.toString().endsWithAny('notifications', 'notifications.git')) {
                        tempDir = Paths.get(tempDir.getAbsolutePath(), 'notifications')
                    }
                    project.exec {
                        workingDir = tempDir
                        def stdout = new ByteArrayOutputStream()
                        executable = (OperatingSystem.current().isWindows()) ? 'gradlew.bat' : './gradlew'
                        args 'assemble'
                        standardOutput stdout
                    }
                    compatibleComponents.add(repositoryUrl)
                } else {
                    logger.lifecycle("Skipping compatibility check for $repositoryUrl")
                }
            } catch (ex) {
                failedComponents.add(repositoryUrl)
                logger.info("Gradle assemble failed for $repositoryUrl", ex)
            } finally {
                tempDir.deleteDir()
            }
        }
        if (!failedComponents.isEmpty()) {
            logger.lifecycle("Incompatible components: $failedComponents")
            logger.info("Compatible components: $compatibleComponents")
        }
        if (!gitFailedComponents.isEmpty()) {
            logger.lifecycle("Components skipped due to git failures: $gitFailedComponents")
            logger.info("Compatible components: $compatibleComponents")
        }
        if (!compatibleComponents.isEmpty()) {
            logger.lifecycle("Compatible components: $compatibleComponents")
        }
    }

    protected static List getRepoUrls() {
        def json = new JsonSlurper().parse(REPO_URL.toURL())
        def repository = json.projects.values()
        def repoUrls = replaceSshWithHttps(repository as List)
        return repoUrls
    }

    protected static replaceSshWithHttps(List<String> repoList) {
        repoList.replaceAll { element ->
            element.replace("git@github.com:", "https://github.com/")
        }
        return repoList
    }

    protected boolean cloneAndCheckout(repoUrl, directory) {
        try {
            def grgit = Grgit.clone(dir: directory, uri: repoUrl)
            def remoteBranches = grgit.branch.list(mode: BranchListOp.Mode.REMOTE)
            String targetBranch = 'origin/' + ref
            if (remoteBranches.find { it.name == targetBranch } == null) {
                gitFailedComponents.add(repoUrl)
                logger.info("$ref does not exist for $repoUrl. Skipping the compatibility check!!")
                return false
            } else {
                logger.info("Checking out $targetBranch")
                grgit.checkout(branch: targetBranch)
                return true
            }
        } catch (ex) {
            logger.error('Exception occurred during GitHub operations', ex)
            gitFailedComponents.add(repoUrl)
            return false
        }
    }
}
