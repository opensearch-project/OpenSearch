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

package org.opensearch.gradle;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;

import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.info.GlobalBuildInfoPlugin;
import org.opensearch.gradle.jvm.JvmTestSuiteHelper;
import org.opensearch.gradle.test.ErrorReportingTestListener;
import org.opensearch.gradle.util.Util;
import org.gradle.api.Action;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.gradle.util.FileUtils.mkdirs;
import static org.opensearch.gradle.util.GradleUtils.maybeConfigure;

/**
 * Applies commonly used settings to all Test tasks in the project
 */
public class OpenSearchTestBasePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        // for fips mode check
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        // Default test task should run only unit tests
        maybeConfigure(project.getTasks(), "test", Test.class, task -> task.include("**/*Tests.class"));

        // none of this stuff is applicable to the `:buildSrc` project tests
        if (project.getPath().equals(":build-tools")) {
            return;
        }

        File heapdumpDir = new File(project.getBuildDir(), "heapdump");

        project.getTasks().withType(Test.class).configureEach(test -> {
            File testOutputDir = new File(test.getReports().getJunitXml().getOutputLocation().getAsFile().get(), "output");

            ErrorReportingTestListener listener = new ErrorReportingTestListener(test.getTestLogging(), test.getLogger(), testOutputDir);
            test.getExtensions().add("errorReportingTestListener", listener);
            test.addTestOutputListener(listener);
            test.addTestListener(listener);

            /*
             * We use lazy-evaluated strings in order to configure system properties whose value will not be known until
             * execution time (e.g. cluster port numbers). Adding these via the normal DSL doesn't work as these get treated
             * as task inputs and therefore Gradle attempts to snapshot them before/after task execution. This fails due
             * to the GStrings containing references to non-serializable objects.
             *
             * We bypass this by instead passing this system properties vi a CommandLineArgumentProvider. This has the added
             * side-effect that these properties are NOT treated as inputs, therefore they don't influence things like the
             * build cache key or up to date checking.
             */
            SystemPropertyCommandLineArgumentProvider nonInputProperties = new SystemPropertyCommandLineArgumentProvider();

            // We specifically use an anonymous inner class here because lambda task actions break Gradle cacheability
            // See: https://docs.gradle.org/current/userguide/more_about_tasks.html#sec:how_does_it_work
            test.doFirst(new Action<Task>() {
                @Override
                public void execute(Task t) {
                    mkdirs(testOutputDir);
                    mkdirs(heapdumpDir);
                    mkdirs(test.getWorkingDir());
                    mkdirs(test.getWorkingDir().toPath().resolve("temp").toFile());

                    // TODO remove once jvm.options are added to test system properties
                    if (BuildParams.getRuntimeJavaVersion() == JavaVersion.VERSION_1_8) {
                        test.systemProperty("java.locale.providers", "SPI,JRE");
                    } else {
                        test.systemProperty("java.locale.providers", "SPI,CLDR");
                        if (test.getJavaVersion().compareTo(JavaVersion.VERSION_17) < 0) {
                            test.jvmArgs("--illegal-access=warn");
                        }
                    }
                    if (test.getJavaVersion().compareTo(JavaVersion.VERSION_17) > 0) {
                        test.jvmArgs("-Djava.security.manager=allow");
                    }
                }
            });
            test.getJvmArgumentProviders().add(nonInputProperties);
            test.getExtensions().add("nonInputProperties", nonInputProperties);

            test.setWorkingDir(project.file(project.getBuildDir() + "/testrun/" + test.getName()));
            test.setMaxParallelForks(Integer.parseInt(System.getProperty("tests.jvms", BuildParams.getDefaultParallel().toString())));

            test.exclude("**/*$*.class");

            test.jvmArgs(
                "-Xmx" + System.getProperty("tests.heap.size", "512m"),
                "-Xms" + System.getProperty("tests.heap.size", "512m"),
                "-XX:+HeapDumpOnOutOfMemoryError"
            );

            test.getJvmArgumentProviders().add(new SimpleCommandLineArgumentProvider("-XX:HeapDumpPath=" + heapdumpDir));

            String argline = System.getProperty("tests.jvm.argline");
            if (argline != null) {
                test.jvmArgs((Object[]) argline.split(" "));
            }

            if (Util.getBooleanProperty("tests.asserts", true)) {
                test.jvmArgs("-ea", "-esa");
            }

            Map<String, String> sysprops = new HashMap<String, String>() {
                {
                    put("java.awt.headless", "true");
                    put("tests.gradle", "true");
                    put("tests.artifact", project.getName());
                    put("tests.task", test.getPath());
                    put("tests.security.manager", "true");
                    put("jna.nosys", "true");
                }
            };
            test.systemProperties(sysprops);

            // ignore changing test seed when build is passed -Dignore.tests.seed for cacheability experimentation
            if (System.getProperty("ignore.tests.seed") != null) {
                nonInputProperties.systemProperty("tests.seed", BuildParams.getTestSeed());
            } else {
                test.systemProperty("tests.seed", BuildParams.getTestSeed());
            }

            // don't track these as inputs since they contain absolute paths and break cache relocatability
            File gradleHome = project.getGradle().getGradleUserHomeDir();
            String gradleVersion = project.getGradle().getGradleVersion();
            nonInputProperties.systemProperty("gradle.dist.lib", new File(project.getGradle().getGradleHomeDir(), "lib"));
            nonInputProperties.systemProperty(
                "gradle.worker.jar",
                gradleHome + "/caches/" + gradleVersion + "/workerMain/gradle-worker.jar"
            );
            nonInputProperties.systemProperty("gradle.user.home", gradleHome);
            // we use 'temp' relative to CWD since this is per JVM and tests are forbidden from writing to CWD
            nonInputProperties.systemProperty("java.io.tmpdir", test.getWorkingDir().toPath().resolve("temp"));

            // TODO: remove setting logging level via system property
            test.systemProperty("tests.logger.level", "WARN");
            System.getProperties().entrySet().forEach(entry -> {
                if ((entry.getKey().toString().startsWith("tests.") || entry.getKey().toString().startsWith("opensearch."))) {
                    test.systemProperty(entry.getKey().toString(), entry.getValue());
                }
            });

            // TODO: remove this once ctx isn't added to update script params in 7.0
            test.systemProperty("opensearch.scripting.update.ctx_in_params", "false");

            // TODO: remove this property in 8.0
            test.systemProperty("opensearch.search.rewrite_sort", "true");

            // TODO: remove this once cname is prepended to transport.publish_address by default in 8.0
            test.systemProperty("opensearch.transport.cname_in_publish_address", "true");

            // Set netty system properties to the properties we configure in jvm.options
            test.systemProperty("io.netty.noUnsafe", "true");
            test.systemProperty("io.netty.noKeySetOptimization", "true");
            test.systemProperty("io.netty.recycler.maxCapacityPerThread", "0");

            test.testLogging(logging -> {
                logging.setShowExceptions(true);
                logging.setShowCauses(true);
                logging.setExceptionFormat("full");
                logging.setShowStandardStreams(Util.getBooleanProperty("tests.output", false));
            });

            if (OS.current().equals(OS.WINDOWS) && System.getProperty("tests.timeoutSuite") == null) {
                // override the suite timeout to 30 mins for windows, because it has the most inefficient filesystem known to man
                test.systemProperty("tests.timeoutSuite", "1800000!");
            }

            /*
             *  If this project builds a shadow JAR than any unit tests should test against that artifact instead of
             *  compiled class output and dependency jars. This better emulates the runtime environment of consumers.
             */
            project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", p -> {
                // Remove output class files and any other dependencies from the test classpath, since the shadow JAR includes these
                FileCollection mainRuntime = project.getExtensions()
                    .getByType(SourceSetContainer.class)
                    .getByName(SourceSet.MAIN_SOURCE_SET_NAME)
                    .getRuntimeClasspath();
                // Add any "shadow" dependencies. These are dependencies that are *not* bundled into the shadow JAR
                Configuration shadowConfig = project.getConfigurations().getByName(ShadowBasePlugin.CONFIGURATION_NAME);
                // Add the shadow JAR artifact itself
                FileCollection shadowJar = project.files(project.getTasks().named("shadowJar"));

                // See please https://docs.gradle.org/8.1/userguide/upgrading_version_8.html#test_task_default_classpath
                test.setClasspath(
                    JvmTestSuiteHelper.getDefaultTestSuite(project)
                        .map(suite -> suite.getSources().getRuntimeClasspath())
                        .orElseGet(() -> test.getClasspath())
                        .minus(mainRuntime)
                        .plus(shadowConfig)
                        .plus(shadowJar)
                );
            });
        });
    }
}
