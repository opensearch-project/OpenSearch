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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.gradle.testfixtures;

import com.avast.gradle.dockercompose.ComposeExtension;
import com.avast.gradle.dockercompose.DockerComposePlugin;
import com.avast.gradle.dockercompose.ServiceInfo;
import com.avast.gradle.dockercompose.tasks.ComposeBuild;
import com.avast.gradle.dockercompose.tasks.ComposeDown;
import com.avast.gradle.dockercompose.tasks.ComposePull;
import com.avast.gradle.dockercompose.tasks.ComposeUp;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.opensearch.gradle.SystemPropertyCommandLineArgumentProvider;
import org.opensearch.gradle.docker.DockerSupportPlugin;
import org.opensearch.gradle.docker.DockerSupportService;
import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.precommit.TestingConventionsTasks;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;

public class TestFixturesPlugin implements Plugin<Project> {

    private static final Logger LOGGER = Logging.getLogger(TestFixturesPlugin.class);
    private static final String DOCKER_COMPOSE_THROTTLE = "dockerComposeThrottle";
    static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

    private static String[] DOCKER_COMPOSE_BINARIES_UNIX = { "/usr/local/bin/docker-compose", "/usr/bin/docker-compose" };

    private static String[] DOCKER_COMPOSE_BINARIES_WINDOWS = {
        System.getenv("PROGRAMFILES") + "\\Docker\\Docker\\resources\\bin\\docker-compose.exe" };

    private static String[] DOCKER_COMPOSE_BINARIES = Os.isFamily(Os.FAMILY_WINDOWS)
        ? DOCKER_COMPOSE_BINARIES_WINDOWS
        : DOCKER_COMPOSE_BINARIES_UNIX;

    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);

        TaskContainer tasks = project.getTasks();
        TestFixtureExtension extension = project.getExtensions().create("testFixtures", TestFixtureExtension.class, project);
        Provider<DockerComposeThrottle> dockerComposeThrottle = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(DOCKER_COMPOSE_THROTTLE, DockerComposeThrottle.class, spec -> spec.getMaxParallelUsages().set(1));

        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        File testfixturesDir = project.file("testfixtures_shared");
        ext.set("testFixturesDir", testfixturesDir);

        if (project.file(DOCKER_COMPOSE_YML).exists()) {
            project.getPluginManager().apply(BasePlugin.class);
            project.getPluginManager().apply(DockerComposePlugin.class);

            TaskProvider<Task> preProcessFixture = project.getTasks().register("preProcessFixture", new Action<Task>() {
                @Override
                public void execute(Task t) {
                    t.getOutputs().dir(testfixturesDir);
                    t.doFirst(new Action<Task>() {
                        @Override
                        public void execute(Task t2) {
                            {
                                try {
                                    Files.createDirectories(testfixturesDir.toPath());
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                        }
                    });
                }
            });
            TaskProvider<Task> buildFixture = project.getTasks().register("buildFixture", new Action<Task>() {
                @Override
                public void execute(Task t) {
                    t.dependsOn(preProcessFixture, tasks.named("composeUp"));
                }
            });

            TaskProvider<Task> postProcessFixture = project.getTasks().register("postProcessFixture", new Action<Task>() {
                @Override
                public void execute(Task task) {
                    task.dependsOn(buildFixture);
                    configureServiceInfoForTask(
                        task,
                        project,
                        false,
                        (name, port) -> task.getExtensions().getByType(ExtraPropertiesExtension.class).set(name, port)
                    );
                }
            });

            maybeSkipTask(dockerSupport, preProcessFixture);
            maybeSkipTask(dockerSupport, postProcessFixture);
            maybeSkipTask(dockerSupport, buildFixture);

            ComposeExtension composeExtension = project.getExtensions().getByType(ComposeExtension.class);
            composeExtension.getUseComposeFiles().set(Collections.singletonList(DOCKER_COMPOSE_YML));
            composeExtension.getRemoveContainers().set(true);

            // Increase the Docker Compose HTTP timeout to 120 sec (the default is 60)
            final Integer timeout = ext.has("dockerComposeHttpTimeout") ? (Integer) ext.get("dockerComposeHttpTimeout") : 120;
            composeExtension.getEnvironment().put("COMPOSE_HTTP_TIMEOUT", timeout);

            Optional<String> dockerCompose = Arrays.asList(DOCKER_COMPOSE_BINARIES)
                .stream()
                .filter(path -> project.file(path).exists())
                .findFirst();

            composeExtension.getExecutable().set(dockerCompose.isPresent() ? dockerCompose.get() : "/usr/bin/docker");
            if (dockerSupport.get().getDockerAvailability().isComposeV2Available) {
                composeExtension.getUseDockerComposeV2().set(true);
            } else if (dockerSupport.get().getDockerAvailability().isComposeAvailable) {
                composeExtension.getUseDockerComposeV2().set(false);
            }

            tasks.named("composeUp").configure(t -> {
                // Avoid running docker-compose tasks in parallel in CI due to some issues on certain Linux distributions
                if (BuildParams.isCi()) {
                    t.usesService(dockerComposeThrottle);
                }
                t.mustRunAfter(preProcessFixture);
            });
            tasks.named("composePull").configure(t -> t.mustRunAfter(preProcessFixture));
            tasks.named("composeDown").configure(t -> t.doLast(t2 -> getFileSystemOperations().delete(d -> d.delete(testfixturesDir))));
        } else {
            project.afterEvaluate(spec -> {
                if (extension.fixtures.isEmpty()) {
                    // if only one fixture is used, that's this one, but without a compose file that's not a valid configuration
                    throw new IllegalStateException(
                        "No " + DOCKER_COMPOSE_YML + " found for " + project.getPath() + " nor does it use other fixtures."
                    );
                }
            });
        }

        extension.fixtures.matching(fixtureProject -> fixtureProject.equals(project) == false)
            .all(fixtureProject -> project.evaluationDependsOn(fixtureProject.getPath()));

        // Skip docker compose tasks if it is unavailable
        maybeSkipTasks(tasks, dockerSupport, Test.class);
        maybeSkipTasks(tasks, dockerSupport, getTaskClass("org.opensearch.gradle.test.RestIntegTestTask"));
        maybeSkipTasks(tasks, dockerSupport, TestingConventionsTasks.class);
        maybeSkipTasks(tasks, dockerSupport, getTaskClass("org.opensearch.gradle.test.AntFixture"));
        maybeSkipTasks(tasks, dockerSupport, ComposeBuild.class);
        maybeSkipTasks(tasks, dockerSupport, ComposeUp.class);
        maybeSkipTasks(tasks, dockerSupport, ComposePull.class);
        maybeSkipTasks(tasks, dockerSupport, ComposeDown.class);

        tasks.withType(Test.class).configureEach(task -> extension.fixtures.all(fixtureProject -> {
            task.dependsOn(fixtureProject.getTasks().named("postProcessFixture"));
            task.finalizedBy(fixtureProject.getTasks().named("composeDown"));
            configureServiceInfoForTask(
                task,
                fixtureProject,
                true,
                (name, host) -> task.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class).systemProperty(name, host)
            );
        }));
    }

    private void maybeSkipTasks(TaskContainer tasks, Provider<DockerSupportService> dockerSupport, Class<? extends DefaultTask> taskClass) {
        tasks.withType(taskClass).configureEach(t -> maybeSkipTask(dockerSupport, t));
    }

    private void maybeSkipTask(Provider<DockerSupportService> dockerSupport, TaskProvider<Task> task) {
        task.configure(t -> maybeSkipTask(dockerSupport, t));
    }

    private void maybeSkipTask(Provider<DockerSupportService> dockerSupport, Task task) {
        task.onlyIf(spec -> {
            boolean isComposeAvailable = dockerSupport.get().getDockerAvailability().isComposeV2Available
                || dockerSupport.get().getDockerAvailability().isComposeAvailable;
            if (isComposeAvailable == false) {
                LOGGER.info("Task {} requires docker-compose but it is unavailable. Task will be skipped.", task.getPath());
            }
            return isComposeAvailable;
        });
    }

    private void configureServiceInfoForTask(
        Task task,
        Project fixtureProject,
        boolean enableFilter,
        BiConsumer<String, Integer> consumer
    ) {
        // Configure ports for the tests as system properties.
        // We only know these at execution time so we need to do it in doFirst
        task.doFirst(new Action<Task>() {
            @Override
            public void execute(Task theTask) {
                TestFixtureExtension extension = theTask.getProject().getExtensions().getByType(TestFixtureExtension.class);

                fixtureProject.getExtensions()
                    .getByType(ComposeExtension.class)
                    .getServicesInfos()
                    .entrySet()
                    .stream()
                    .filter(entry -> enableFilter == false || extension.isServiceRequired(entry.getKey(), fixtureProject.getPath()))
                    .forEach(entry -> {
                        String service = entry.getKey();
                        ServiceInfo infos = entry.getValue();
                        infos.getTcpPorts().forEach((container, host) -> {
                            String name = "test.fixtures." + service + ".tcp." + container;
                            theTask.getLogger().info("port mapping property: {}={}", name, host);
                            consumer.accept(name, host);
                        });
                        infos.getUdpPorts().forEach((container, host) -> {
                            String name = "test.fixtures." + service + ".udp." + container;
                            theTask.getLogger().info("port mapping property: {}={}", name, host);
                            consumer.accept(name, host);
                        });
                    });
            }
        });
    }

    @SuppressWarnings("unchecked")
    private Class<? extends DefaultTask> getTaskClass(String type) {
        Class<?> aClass;
        try {
            aClass = Class.forName(type);
            if (DefaultTask.class.isAssignableFrom(aClass) == false) {
                throw new IllegalArgumentException("Not a task type: " + type);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No such task: " + type);
        }
        return (Class<? extends DefaultTask>) aClass;
    }

}
