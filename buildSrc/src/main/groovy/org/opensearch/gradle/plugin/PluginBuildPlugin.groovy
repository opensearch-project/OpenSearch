/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.opensearch.gradle.plugin

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import org.gradle.api.tasks.bundling.AbstractArchiveTask
import org.opensearch.gradle.BuildPlugin
import org.opensearch.gradle.NoticeTask
import org.opensearch.gradle.Version
import org.opensearch.gradle.VersionProperties
import org.opensearch.gradle.dependencies.CompileOnlyResolvePlugin
import org.opensearch.gradle.info.BuildParams
import org.opensearch.gradle.test.RestTestBasePlugin
import org.opensearch.gradle.testclusters.RunTask
import org.opensearch.gradle.util.Util
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.BasePlugin
import org.gradle.api.plugins.BasePluginExtension
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin
import org.gradle.api.publish.maven.tasks.GenerateMavenPom
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.bundling.Zip
import org.gradle.jvm.tasks.Jar

/**
 * Encapsulates build configuration for an OpenSearch plugin.
 */
class PluginBuildPlugin implements Plugin<Project> {

    public static final String PLUGIN_EXTENSION_NAME = 'opensearchplugin'

    @Override
    void apply(Project project) {
        project.pluginManager.apply(BuildPlugin)
        project.pluginManager.apply(RestTestBasePlugin)
        project.pluginManager.apply(CompileOnlyResolvePlugin.class);

        PluginPropertiesExtension extension = project.extensions.create(PLUGIN_EXTENSION_NAME, PluginPropertiesExtension, project)
        configureDependencies(project)

        boolean isModule = project.path.startsWith(':modules:')

        createBundleTasks(project, extension)

        project.afterEvaluate {
            project.extensions.getByType(PluginPropertiesExtension).extendedPlugins.each { pluginName ->
                // Auto add dependent modules to the test cluster
                if (project.findProject(":modules:${pluginName}") != null) {
                    project.testClusters.all {
                        module(":modules:${pluginName}")
                    }
                }
            }
            PluginPropertiesExtension extension1 = project.getExtensions().getByType(PluginPropertiesExtension.class)
            configurePublishing(project, extension1)
            String name = extension1.name

            BasePluginExtension base = project.getExtensions().findByType(BasePluginExtension.class)
            base.archivesName = name
            project.description = extension1.description

            if (extension1.name == null) {
                throw new InvalidUserDataException('name is a required setting for opensearchplugin')
            }
            if (extension1.description == null) {
                throw new InvalidUserDataException('description is a required setting for opensearchplugin')
            }
            if (extension1.classname == null) {
                throw new InvalidUserDataException('classname is a required setting for opensearchplugin')
            }

            JavaPluginExtension java = project.getExtensions().findByType(JavaPluginExtension.class)
            Map<String, String> properties = [
                    'name'                : extension1.name,
                    'description'         : extension1.description,
                    'version'             : extension1.version,
                    'opensearchVersion'   : Version.fromString(VersionProperties.getOpenSearch()).toString(),
                    'javaVersion'         : java.targetCompatibility as String,
                    'classname'           : extension1.classname,
                    'customFolderName'    : extension1.customFolderName,
                    'extendedPlugins'     : extension1.extendedPlugins.join(','),
                    'hasNativeController' : extension1.hasNativeController,
                    'requiresKeystore'    : extension1.requiresKeystore
            ]
            project.tasks.named('pluginProperties').configure {
                expand(properties)
                inputs.properties(properties)
            }
            if (isModule == false) {
                addNoticeGeneration(project, extension1)
            }
        }

        project.tasks.named('testingConventions').configure {
            naming.clear()
            naming {
                Tests {
                    baseClass 'org.apache.lucene.tests.util.LuceneTestCase'
                }
                IT {
                    baseClass 'org.opensearch.test.OpenSearchIntegTestCase'
                    baseClass 'org.opensearch.test.rest.OpenSearchRestTestCase'
                    baseClass 'org.opensearch.test.OpenSearchSingleNodeTestCase'
                }
            }
        }
        project.configurations.getByName('default')
                .extendsFrom(project.configurations.getByName('runtimeClasspath'))
        project.tasks.withType(AbstractArchiveTask.class).configureEach { task ->
            // ignore file timestamps
            // be consistent in archive file order
            task.preserveFileTimestamps = false
            task.reproducibleFileOrder = true
        }
        // allow running ES with this plugin in the foreground of a build
        project.tasks.register('run', RunTask) {
            dependsOn(project.tasks.bundlePlugin)
        }
    }

    private void configurePublishing(Project project, PluginPropertiesExtension extension) {
        // Only configure publishing if applied externally
        if (extension.hasClientJar) {
            project.pluginManager.apply('com.netflix.nebula.maven-base-publish')
            // Only change Jar tasks, we don't want a -client zip so we can't change archivesName
            project.tasks.withType(Jar) {
                archiveBaseName = archiveBaseName.get() +  "-client"
            }
            // always configure publishing for client jars
            project.publishing.publications.nebula(MavenPublication).artifactId = extension.name + "-client"
            final BasePluginExtension base = project.getExtensions().findByType(BasePluginExtension.class)
            project.tasks.withType(GenerateMavenPom.class).configureEach { GenerateMavenPom generatePOMTask ->
                generatePOMTask.destination = "${project.buildDir}/distributions/${base.archivesName}-client-${project.versions.opensearch}.pom"
            }
        } else {
            if (project.plugins.hasPlugin(MavenPublishPlugin)) {
                project.publishing.publications.nebula(MavenPublication).artifactId = extension.name
            }
        }
    }

    private static void configureDependencies(Project project) {
        project.dependencies {
            if (BuildParams.internal) {
                compileOnly project.project(':server')
                testImplementation project.project(':test:framework')
            } else {
                compileOnly "org.opensearch:opensearch:${project.versions.opensearch}"
                testImplementation "org.opensearch.test:framework:${project.versions.opensearch}"
            }
            // we "upgrade" these optional deps to provided for plugins, since they will run
            // with a full opensearch server that includes optional deps
            compileOnly "org.locationtech.spatial4j:spatial4j:${project.versions.spatial4j}"
            compileOnly "org.locationtech.jts:jts-core:${project.versions.jts}"
            compileOnly "org.apache.logging.log4j:log4j-api:${project.versions.log4j}"
            compileOnly "org.apache.logging.log4j:log4j-core:${project.versions.log4j}"
            compileOnly "net.java.dev.jna:jna:${project.versions.jna}"
        }
    }

    /**
     * Adds a bundlePlugin task which builds the zip containing the plugin jars,
     * metadata, properties, and packaging files
     */
    private static void createBundleTasks(Project project, PluginPropertiesExtension extension) {
        File pluginMetadata = project.file('src/main/plugin-metadata')
        File templateFile = new File(project.buildDir, "templates/plugin-descriptor.properties")

        // create tasks to build the properties file for this plugin
        TaskProvider<Task> copyPluginPropertiesTemplate = project.tasks.register('copyPluginPropertiesTemplate') {
            outputs.file(templateFile)
            doLast {
                InputStream resourceTemplate = PluginBuildPlugin.getResourceAsStream("/${templateFile.name}")
                templateFile.setText(resourceTemplate.getText('UTF-8'), 'UTF-8')
            }
        }

        TaskProvider<Copy> buildProperties = project.tasks.register('pluginProperties', Copy) {
            dependsOn(copyPluginPropertiesTemplate)
            from(templateFile)
            into("${project.buildDir}/generated-resources")
        }

        // add the plugin properties and metadata to test resources, so unit tests can
        // know about the plugin (used by test security code to statically initialize the plugin in unit tests)
        SourceSet testSourceSet = project.sourceSets.test
        testSourceSet.output.dir("${project.buildDir}/generated-resources", builtBy: buildProperties)
        testSourceSet.resources.srcDir(pluginMetadata)

        // create the actual bundle task, which zips up all the files for the plugin
        TaskProvider<Zip> bundle = project.tasks.register('bundlePlugin', Zip) {
            from buildProperties
            from pluginMetadata // metadata (eg custom security policy)
            /*
             * If the plugin is using the shadow plugin then we need to bundle
             * that shadow jar.
             */
            from { project.plugins.hasPlugin(ShadowPlugin) ? project.shadowJar : project.jar }
            from project.configurations.runtimeClasspath - project.configurations.getByName(
                    CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME
            )
            // extra files for the plugin to go into the zip
            from('src/main/packaging') // TODO: move all config/bin/_size/etc into packaging
            from('src/main') {
                include 'config/**'
                include 'bin/**'
            }
        }
        project.tasks.named(BasePlugin.ASSEMBLE_TASK_NAME).configure {
            dependsOn(bundle)
        }

        // also make the zip available as a configuration (used when depending on this project)
        project.configurations.create('zip')
        project.artifacts.add('zip', bundle)
    }

    /** Configure the pom for the main jar of this plugin */
    protected static void addNoticeGeneration(Project project, PluginPropertiesExtension extension) {
        File licenseFile = extension.licenseFile
        if (licenseFile != null) {
            project.tasks.named('bundlePlugin').configure {
                from(licenseFile.parentFile) {
                    include(licenseFile.name)
                    rename { 'LICENSE.txt' }
                }
            }
        }
        File noticeFile = extension.noticeFile
        if (noticeFile != null) {
            TaskProvider<NoticeTask> generateNotice = project.tasks.register('generateNotice', NoticeTask) {
                inputFile = noticeFile
                source(Util.getJavaMainSourceSet(project).get().allJava)
            }
            project.tasks.named('bundlePlugin').configure {
                from(generateNotice)
            }
        }
    }
}
