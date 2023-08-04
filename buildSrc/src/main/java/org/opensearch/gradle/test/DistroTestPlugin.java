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

package org.opensearch.gradle.test;

import org.opensearch.gradle.Architecture;
import org.opensearch.gradle.DistributionDownloadPlugin;
import org.opensearch.gradle.JavaPackageType;
import org.opensearch.gradle.OpenSearchDistribution;
import org.opensearch.gradle.Jdk;
import org.opensearch.gradle.JdkDownloadPlugin;
import org.opensearch.gradle.SystemPropertyCommandLineArgumentProvider;
import org.opensearch.gradle.Version;
import org.opensearch.gradle.VersionProperties;
import org.opensearch.gradle.docker.DockerSupportPlugin;
import org.opensearch.gradle.docker.DockerSupportService;
import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.internal.InternalDistributionDownloadPlugin;
import org.opensearch.gradle.util.GradleUtils;
import org.opensearch.gradle.util.Util;
import org.opensearch.gradle.vagrant.VagrantBasePlugin;
import org.opensearch.gradle.vagrant.VagrantExtension;
import org.gradle.api.Action;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.specs.Specs;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.opensearch.gradle.vagrant.VagrantMachine;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class DistroTestPlugin implements Plugin<Project> {
    private static final String SYSTEM_JDK_VERSION = "11.0.20+8";
    private static final String SYSTEM_JDK_VENDOR = "adoptium";
    private static final String GRADLE_JDK_VERSION = "17.0.8+7";
    private static final String GRADLE_JDK_VENDOR = "adoptium";

    // all distributions used by distro tests. this is temporary until tests are per distribution
    private static final String EXAMPLE_PLUGIN_CONFIGURATION = "examplePlugin";
    private static final String IN_VM_SYSPROP = "tests.inVM";
    private static final String DISTRIBUTION_SYSPROP = "tests.distribution";
    private static final String BWC_DISTRIBUTION_SYSPROP = "tests.bwc-distribution";
    private static final String EXAMPLE_PLUGIN_SYSPROP = "tests.example-plugin";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        project.getPlugins().apply(InternalDistributionDownloadPlugin.class);
        project.getPlugins().apply(JdkDownloadPlugin.class);
        project.getPluginManager().apply("opensearch.build");

        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        // TODO: it would be useful to also have the SYSTEM_JAVA_HOME setup in the root project, so that running from GCP only needs
        // a java for gradle to run, and the tests are self sufficient and consistent with the java they use
        NamedDomainObjectContainer<OpenSearchDistribution> allDistributions = DistributionDownloadPlugin.getContainer(project);
        List<OpenSearchDistribution> testDistributions = configureDistributions(project);

        Map<OpenSearchDistribution.Type, TaskProvider<?>> lifecycleTasks = lifecycleTasks(project, "destructiveDistroTest");
        Map<String, TaskProvider<?>> versionTasks = versionTasks(project, "destructiveDistroUpgradeTest");
        TaskProvider<Task> destructiveDistroTest = project.getTasks().register("destructiveDistroTest");

        Configuration examplePlugin = configureExamplePlugin(project);

        List<TaskProvider<Test>> windowsTestTasks = new ArrayList<>();
        Map<OpenSearchDistribution.Type, List<TaskProvider<Test>>> linuxTestTasks = new HashMap<>();
        Map<String, List<TaskProvider<Test>>> upgradeTestTasks = new HashMap<>();
        Map<String, TaskProvider<?>> depsTasks = new HashMap<>();
        for (OpenSearchDistribution distribution : testDistributions) {
            String taskname = destructiveDistroTestTaskName(distribution);
            TaskProvider<?> depsTask = project.getTasks().register(taskname + "#deps");
            depsTask.configure(t -> t.dependsOn(distribution, examplePlugin));
            depsTasks.put(taskname, depsTask);
            // Avoid duplicate tasks such as docker registered in lifecycleTasks
            if (project.getTasksByName(taskname, false).isEmpty()) {
                TaskProvider<Test> destructiveTask = configureTestTask(project, taskname, distribution, t -> {
                    t.onlyIf(t2 -> distribution.isDocker() == false || dockerSupport.get().getDockerAvailability().isAvailable);
                    addSysprop(t, DISTRIBUTION_SYSPROP, distribution::getFilepath);
                    addSysprop(t, EXAMPLE_PLUGIN_SYSPROP, () -> examplePlugin.getSingleFile().toString());
                    t.exclude("**/PackageUpgradeTests.class");
                }, depsTask);
                if (distribution.getPlatform() == OpenSearchDistribution.Platform.WINDOWS) {
                    windowsTestTasks.add(destructiveTask);
                } else {
                    linuxTestTasks.computeIfAbsent(distribution.getType(), k -> new ArrayList<>()).add(destructiveTask);
                }
                destructiveDistroTest.configure(t -> t.dependsOn(destructiveTask));
                lifecycleTasks.get(distribution.getType()).configure(t -> t.dependsOn(destructiveTask));
            }

            if ((distribution.getType() == OpenSearchDistribution.Type.DEB || distribution.getType() == OpenSearchDistribution.Type.RPM)
                && distribution.getBundledJdk() != JavaPackageType.NONE) {
                for (Version version : BuildParams.getBwcVersions().getIndexCompatible()) {
                    if (version.before("6.3.0")) {
                        continue; // before opening xpack
                    }
                    final OpenSearchDistribution bwcDistro;
                    if (version.equals(Version.fromString(distribution.getVersion()))) {
                        // this is the same as the distribution we are testing
                        bwcDistro = distribution;
                    } else {
                        bwcDistro = createDistro(
                            allDistributions,
                            distribution.getArchitecture(),
                            distribution.getType(),
                            distribution.getPlatform(),
                            distribution.getBundledJdk(),
                            version.toString()
                        );

                    }
                    String upgradeTaskname = destructiveDistroUpgradeTestTaskName(distribution, version.toString());
                    TaskProvider<?> upgradeDepsTask = project.getTasks().register(upgradeTaskname + "#deps");
                    upgradeDepsTask.configure(t -> t.dependsOn(distribution, bwcDistro));
                    depsTasks.put(upgradeTaskname, upgradeDepsTask);
                    TaskProvider<Test> upgradeTest = configureTestTask(project, upgradeTaskname, distribution, t -> {
                        addSysprop(t, DISTRIBUTION_SYSPROP, distribution::getFilepath);
                        addSysprop(t, BWC_DISTRIBUTION_SYSPROP, bwcDistro::getFilepath);
                        t.include("**/PackageUpgradeTests.class");
                    }, upgradeDepsTask);
                    versionTasks.get(version.toString()).configure(t -> t.dependsOn(upgradeTest));
                    upgradeTestTasks.computeIfAbsent(version.toString(), k -> new ArrayList<>()).add(upgradeTest);
                }
            }
        }

        // setup jdks used by no-jdk tests, and by gradle executing
        TaskProvider<Copy> linuxGradleJdk = createJdk(project, "gradle", GRADLE_JDK_VENDOR, GRADLE_JDK_VERSION, "linux", "x64");
        TaskProvider<Copy> linuxSystemJdk = createJdk(project, "system", SYSTEM_JDK_VENDOR, SYSTEM_JDK_VERSION, "linux", "x64");
        TaskProvider<Copy> windowsGradleJdk = createJdk(project, "gradle", GRADLE_JDK_VENDOR, GRADLE_JDK_VERSION, "windows", "x64");
        TaskProvider<Copy> windowsSystemJdk = createJdk(project, "system", SYSTEM_JDK_VENDOR, SYSTEM_JDK_VERSION, "windows", "x64");

        project.subprojects(vmProject -> {
            vmProject.getPluginManager().apply(VagrantBasePlugin.class);
            TaskProvider<Copy> gradleJdk = isWindows(vmProject) ? windowsGradleJdk : linuxGradleJdk;
            TaskProvider<Copy> systemJdk = isWindows(vmProject) ? windowsSystemJdk : linuxSystemJdk;
            configureVM(vmProject, gradleJdk, systemJdk);
            List<Object> vmDependencies = Arrays.asList(
                gradleJdk,
                systemJdk,
                project.getConfigurations().getByName("testRuntimeClasspath")
            );

            Map<OpenSearchDistribution.Type, TaskProvider<?>> vmLifecyleTasks = lifecycleTasks(vmProject, "distroTest");
            Map<String, TaskProvider<?>> vmVersionTasks = versionTasks(vmProject, "distroUpgradeTest");
            TaskProvider<Task> distroTest = vmProject.getTasks().register("distroTest");

            // windows boxes get windows distributions, and linux boxes get linux distributions
            if (isWindows(vmProject)) {
                configureVMWrapperTasks(vmProject, windowsTestTasks, depsTasks, wrapperTask -> {
                    vmLifecyleTasks.get(OpenSearchDistribution.Type.ARCHIVE).configure(t -> t.dependsOn(wrapperTask));
                }, vmDependencies);
            } else {
                for (Entry<OpenSearchDistribution.Type, List<TaskProvider<Test>>> entry : linuxTestTasks.entrySet()) {
                    OpenSearchDistribution.Type type = entry.getKey();
                    TaskProvider<?> vmLifecycleTask = vmLifecyleTasks.get(type);
                    configureVMWrapperTasks(vmProject, entry.getValue(), depsTasks, wrapperTask -> {
                        vmLifecycleTask.configure(t -> t.dependsOn(wrapperTask));

                        // Only VM sub-projects that are specifically opted-in to testing Docker should
                        // have the Docker task added as a dependency. Although we control whether Docker
                        // is installed in the VM via `Vagrantfile` and we could auto-detect its presence
                        // in the VM, the test tasks e.g. `destructiveDistroTest.default-docker` are defined
                        // on the host during Gradle's configuration phase and not in the VM, so
                        // auto-detection doesn't work.
                        //
                        // The shouldTestDocker property could be null, hence we use Boolean.TRUE.equals()
                        boolean shouldExecute = (type != OpenSearchDistribution.Type.DOCKER)
                            || Boolean.TRUE.equals(vmProject.findProperty("shouldTestDocker"));

                        if (shouldExecute) {
                            distroTest.configure(t -> t.dependsOn(wrapperTask));
                        }
                    }, vmDependencies);
                }

                for (Entry<String, List<TaskProvider<Test>>> entry : upgradeTestTasks.entrySet()) {
                    String version = entry.getKey();
                    TaskProvider<?> vmVersionTask = vmVersionTasks.get(version);
                    configureVMWrapperTasks(
                        vmProject,
                        entry.getValue(),
                        depsTasks,
                        wrapperTask -> { vmVersionTask.configure(t -> t.dependsOn(wrapperTask)); },
                        vmDependencies
                    );
                }
            }
        });
    }

    private static Map<OpenSearchDistribution.Type, TaskProvider<?>> lifecycleTasks(Project project, String taskPrefix) {
        Map<OpenSearchDistribution.Type, TaskProvider<?>> lifecyleTasks = new HashMap<>();

        lifecyleTasks.put(OpenSearchDistribution.Type.DOCKER, project.getTasks().register(taskPrefix + ".docker"));
        lifecyleTasks.put(OpenSearchDistribution.Type.ARCHIVE, project.getTasks().register(taskPrefix + ".archives"));
        lifecyleTasks.put(OpenSearchDistribution.Type.DEB, project.getTasks().register(taskPrefix + ".packages"));
        lifecyleTasks.put(OpenSearchDistribution.Type.RPM, lifecyleTasks.get(OpenSearchDistribution.Type.DEB));

        return lifecyleTasks;
    }

    private static Map<String, TaskProvider<?>> versionTasks(Project project, String taskPrefix) {
        Map<String, TaskProvider<?>> versionTasks = new HashMap<>();

        for (Version version : BuildParams.getBwcVersions().getIndexCompatible()) {
            versionTasks.put(version.toString(), project.getTasks().register(taskPrefix + ".v" + version));
        }

        return versionTasks;
    }

    private static TaskProvider<Copy> createJdk(
        Project project,
        String purpose,
        String vendor,
        String version,
        String platform,
        String architecture
    ) {
        Jdk jdk = JdkDownloadPlugin.getContainer(project).create(platform + "-" + purpose);
        jdk.setVendor(vendor);
        jdk.setVersion(version);
        jdk.setPlatform(platform);
        jdk.setArchitecture(architecture);

        String taskname = "copy" + Util.capitalize(platform) + Util.capitalize(purpose) + "Jdk";
        TaskProvider<Copy> copyTask = project.getTasks().register(taskname, Copy.class);
        copyTask.configure(t -> {
            t.from(jdk);
            t.into(new File(project.getBuildDir(), "jdks/" + platform + "-" + architecture + "-" + vendor + "-" + version));
        });
        return copyTask;
    }

    private static void configureVM(Project project, TaskProvider<Copy> gradleJdkProvider, TaskProvider<Copy> systemJdkProvider) {
        String box = project.getName();

        // setup VM used by these tests
        VagrantExtension vagrant = project.getExtensions().getByType(VagrantExtension.class);
        vagrant.setBox(box);
        vagrant.vmEnv("SYSTEM_JAVA_HOME", convertPath(project, vagrant, systemJdkProvider, "", ""));
        // set java home for gradle to use. package tests will overwrite/remove this for each test case
        vagrant.vmEnv("JAVA_HOME", convertPath(project, vagrant, gradleJdkProvider, "", ""));
        if (System.getenv("JENKINS_URL") != null) {
            Stream.of("JOB_NAME", "JENKINS_URL", "BUILD_NUMBER", "BUILD_URL").forEach(name -> vagrant.vmEnv(name, System.getenv(name)));
        }
        vagrant.setIsWindowsVM(isWindows(project));
    }

    private static Object convertPath(
        Project project,
        VagrantExtension vagrant,
        TaskProvider<Copy> jdkProvider,
        String additionaLinux,
        String additionalWindows
    ) {
        return Util.toStringable(() -> {
            String hostPath = jdkProvider.get().getDestinationDir().toString();
            if (vagrant.isWindowsVM()) {
                return VagrantMachine.convertWindowsPath(project, hostPath) + additionalWindows;
            } else {
                return VagrantMachine.convertLinuxPath(project, hostPath) + additionaLinux;
            }
        });
    }

    private static Configuration configureExamplePlugin(Project project) {
        Configuration examplePlugin = project.getConfigurations().create(EXAMPLE_PLUGIN_CONFIGURATION);
        DependencyHandler deps = project.getDependencies();
        Map<String, String> examplePluginProject = new HashMap<String, String>() {
            {
                put("path", ":example-plugins:custom-settings");
                put("configuration", "zip");
            }
        };
        deps.add(EXAMPLE_PLUGIN_CONFIGURATION, deps.project(examplePluginProject));
        return examplePlugin;
    }

    private static void configureVMWrapperTasks(
        Project project,
        List<TaskProvider<Test>> destructiveTasks,
        Map<String, TaskProvider<?>> depsTasks,
        Action<TaskProvider<GradleDistroTestTask>> configure,
        Object... additionalDeps
    ) {
        for (TaskProvider<? extends Task> destructiveTask : destructiveTasks) {
            String destructiveTaskName = destructiveTask.getName();
            String taskname = destructiveTaskName.substring("destructive".length());
            taskname = taskname.substring(0, 1).toLowerCase(Locale.ROOT) + taskname.substring(1);
            TaskProvider<GradleDistroTestTask> vmTask = project.getTasks().register(taskname, GradleDistroTestTask.class, t -> {
                t.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
                t.setDescription("Runs " + destructiveTaskName.split("\\.", 2)[1] + " tests within vagrant");
                t.setTaskName(destructiveTaskName);
                t.extraArg("-D'" + IN_VM_SYSPROP + "'");
                t.dependsOn(depsTasks.get(destructiveTaskName));
                t.dependsOn(additionalDeps);
            });
            configure.execute(vmTask);
        }
    }

    private static TaskProvider<Test> configureTestTask(
        Project project,
        String taskname,
        OpenSearchDistribution distribution,
        Action<? super Test> configure,
        Object... deps
    ) {
        return project.getTasks().register(taskname, Test.class, t -> {
            // Only run tests for the current architecture
            t.onlyIf(t3 -> distribution.getArchitecture() == Architecture.current());
            t.getOutputs().doNotCacheIf("Build cache is disabled for packaging tests", Specs.satisfyAll());
            t.setMaxParallelForks(1);
            t.setWorkingDir(project.getProjectDir());
            if (System.getProperty(IN_VM_SYSPROP) == null) {
                t.dependsOn(deps);
            }
            configure.execute(t);
        });
    }

    private List<OpenSearchDistribution> configureDistributions(Project project) {
        NamedDomainObjectContainer<OpenSearchDistribution> distributions = DistributionDownloadPlugin.getContainer(project);
        List<OpenSearchDistribution> currentDistros = new ArrayList<>();

        for (Architecture architecture : Architecture.values()) {
            for (OpenSearchDistribution.Type type : Arrays.asList(
                OpenSearchDistribution.Type.DEB,
                OpenSearchDistribution.Type.RPM,
                OpenSearchDistribution.Type.DOCKER
            )) {
                for (JavaPackageType bundledJdk : Set.of(JavaPackageType.NONE, JavaPackageType.JDK)) {
                    if (bundledJdk == JavaPackageType.NONE) {
                        // We'll never publish an ARM (arm64) build without a bundled JDK.
                        if (architecture == Architecture.ARM64) {
                            continue;
                        }
                        // All our Docker images include a bundled JDK so it doesn't make sense to test without one.
                        if (type == OpenSearchDistribution.Type.DOCKER) {
                            continue;
                        }
                    }

                    currentDistros.add(
                        createDistro(distributions, architecture, type, null, bundledJdk, VersionProperties.getOpenSearch())
                    );
                }
            }
        }

        for (Architecture architecture : Architecture.values()) {
            for (OpenSearchDistribution.Platform platform : Arrays.asList(
                OpenSearchDistribution.Platform.LINUX,
                OpenSearchDistribution.Platform.WINDOWS
            )) {
                for (JavaPackageType bundledJdk : Set.of(JavaPackageType.NONE, JavaPackageType.JDK)) {
                    if (bundledJdk == JavaPackageType.NONE && architecture != Architecture.X64) {
                        // We will never publish distributions for non-x86 (amd64) platforms
                        // without a bundled JDK
                        continue;
                    }

                    currentDistros.add(
                        createDistro(
                            distributions,
                            architecture,
                            OpenSearchDistribution.Type.ARCHIVE,
                            platform,
                            bundledJdk,
                            VersionProperties.getOpenSearch()
                        )
                    );
                }
            }
        }

        return currentDistros;
    }

    private static OpenSearchDistribution createDistro(
        NamedDomainObjectContainer<OpenSearchDistribution> distributions,
        Architecture architecture,
        OpenSearchDistribution.Type type,
        OpenSearchDistribution.Platform platform,
        JavaPackageType bundledJdk,
        String version
    ) {
        String name = distroId(type, platform, bundledJdk, architecture) + "-" + version;
        boolean isDocker = type == OpenSearchDistribution.Type.DOCKER;
        OpenSearchDistribution distro = distributions.create(name, d -> {
            d.setArchitecture(architecture);
            d.setType(type);
            if (type == OpenSearchDistribution.Type.ARCHIVE) {
                d.setPlatform(platform);
            }
            if (isDocker == false) {
                d.setBundledJdk(bundledJdk);
            }
            d.setVersion(version);
        });

        // Allow us to gracefully omit building Docker distributions if Docker is not available on the system.
        // In such a case as we can't build the Docker images we'll simply skip the corresponding tests.
        if (isDocker) {
            distro.setFailIfUnavailable(false);
        }

        return distro;
    }

    // return true if the project is for a windows VM, false otherwise
    private static boolean isWindows(Project project) {
        return project.getName().contains("windows");
    }

    private static String distroId(
        OpenSearchDistribution.Type type,
        OpenSearchDistribution.Platform platform,
        JavaPackageType bundledJdk,
        Architecture architecture
    ) {
        return (type == OpenSearchDistribution.Type.ARCHIVE ? platform + "-" : "") + type + (bundledJdk != JavaPackageType.NONE
            ? (bundledJdk == JavaPackageType.JDK ? "" : "-jre")
            : "-no-jdk") + (architecture == Architecture.X64 ? "" : "-" + architecture.toString().toLowerCase());
    }

    private static String destructiveDistroTestTaskName(OpenSearchDistribution distro) {
        OpenSearchDistribution.Type type = distro.getType();
        return "destructiveDistroTest." + distroId(type, distro.getPlatform(), distro.getBundledJdk(), distro.getArchitecture());
    }

    private static String destructiveDistroUpgradeTestTaskName(OpenSearchDistribution distro, String bwcVersion) {
        OpenSearchDistribution.Type type = distro.getType();
        return "destructiveDistroUpgradeTest.v"
            + bwcVersion
            + "."
            + distroId(type, distro.getPlatform(), distro.getBundledJdk(), distro.getArchitecture());
    }

    private static void addSysprop(Test task, String sysprop, Supplier<String> valueSupplier) {
        SystemPropertyCommandLineArgumentProvider props = task.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class);
        props.systemProperty(sysprop, valueSupplier);
    }
}
