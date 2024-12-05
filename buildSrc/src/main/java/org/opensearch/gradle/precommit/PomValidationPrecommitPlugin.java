/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gradle.precommit;

import org.opensearch.gradle.util.Util;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.TaskProvider;

/**
 * Adds pom validation to every pom generation task.
 */
public class PomValidationPrecommitPlugin extends PrecommitPlugin {

    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        TaskProvider<Task> validatePom = project.getTasks().register("validatePom");
        PublishingExtension publishing = project.getExtensions().getByType(PublishingExtension.class);
        publishing.getPublications().all(publication -> {
            String publicationName = Util.capitalize(publication.getName());
            TaskProvider<PomValidationTask> validateTask = project.getTasks()
                .register("validate" + publicationName + "Pom", PomValidationTask.class);
            validatePom.configure(t -> t.dependsOn(validateTask));
            TaskProvider<GenerateMavenPom> generateMavenPom = project.getTasks()
                .withType(GenerateMavenPom.class)
                .named("generatePomFileFor" + publicationName + "Publication");
            validateTask.configure(task -> {
                task.dependsOn(generateMavenPom);
                task.getPomFile().fileProvider(generateMavenPom.map(GenerateMavenPom::getDestination));
                publishing.getPublications().all(publicationForPomGen -> {
                    task.mustRunAfter(
                        project.getTasks()
                            .withType(GenerateMavenPom.class)
                            .getByName("generatePomFileFor" + Util.capitalize(publicationForPomGen.getName()) + "Publication")
                    );
                });
            });
        });

        return validatePom;
    }
}
