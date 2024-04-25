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
 * Copyright 2014-2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.ivy.IvyPublication
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.plugins.PublishingPlugin

/**
 * Including the support of `optional` dependencies: https://github.com/nebula-plugins/gradle-extra-configurations-plugin/blob/v9.0.0/src/main/groovy/nebula/plugin/extraconfigurations/OptionalBasePlugin.groovy
 */
class OptionalDependenciesPlugin implements Plugin<Project> {
    static final String OPTIONAL_IDENTIFIER = 'optional'

    @Override
    void apply(Project project) {
        enhanceProjectModel(project)
        configureMavenPublishPlugin(project)
        configureIvyPublishPlugin(project)
    }

    /**
     * Enhances the Project domain object by adding
     *
     * a) a extra property List that holds optional dependencies
     * b) a extra method that can be executed as parameter when declaring dependencies
     *
     * @param project Project
     */
    private void enhanceProjectModel(Project project) {
        project.ext.optionalDeps = []

        project.ext.optional = { dep ->
            project.ext.optionalDeps << dep
        }
    }

    /**
     * Configures Maven Publishing plugin to ensure that published dependencies receive the optional element.
     *
     * @param project Project
     */
    private void configureMavenPublishPlugin(Project project) {
        project.plugins.withType(PublishingPlugin) {
            project.publishing {
                publications {
                    project.extensions.findByType(PublishingExtension)?.publications?.withType(MavenPublication) { MavenPublication pub ->
                        pub.pom.withXml {
                            project.ext.optionalDeps.each { dep ->
                                def foundDep = asNode().dependencies.dependency.find {
                                    it.groupId.text() == dep.group && it.artifactId.text() == dep.name
                                }

                                if (foundDep) {
                                    if (foundDep.optional) {
                                        foundDep.optional.value = 'true'
                                    } else {
                                        foundDep.appendNode(OPTIONAL_IDENTIFIER, 'true')
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Configures Ivy Publishing plugin to ensure that published dependencies receive the correct conf attribute value.
     *
     * @param project Project
     */
    private void configureIvyPublishPlugin(Project project) {
        project.plugins.withType(PublishingPlugin) {
            project.publishing {
                publications {
                    project.extensions.findByType(PublishingExtension)?.publications?.withType(IvyPublication) { IvyPublication pub ->
                        pub.descriptor.withXml {
                            def rootNode = asNode()

                            // Add optional configuration if it doesn't exist yet
                            if (!rootNode.configurations.find { it.@name == OPTIONAL_IDENTIFIER }) {
                                rootNode.configurations[0].appendNode('conf', [name: OPTIONAL_IDENTIFIER, visibility: 'public'])
                            }

                            // Replace dependency "runtime->default" conf attribute value with "optional"
                            project.ext.optionalDeps.each { dep ->
                                def foundDep = rootNode.dependencies.dependency.find { it.@name == dep.name }
                                foundDep?.@conf = OPTIONAL_IDENTIFIER
                            }
                        }
                    }
                }
            }
        }
    }
}
