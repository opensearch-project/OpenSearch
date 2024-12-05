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

package org.opensearch.gradle.internal;

import org.opensearch.gradle.BwcVersions;
import org.opensearch.gradle.LoggedExec;
import org.opensearch.gradle.Version;
import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.Action;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import javax.inject.Inject;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;

/**
 * We want to be able to do BWC tests for unreleased versions without relying on and waiting for snapshots.
 * For this we need to check out and build the unreleased versions.
 * Since these depend on the current version, we can't name the Gradle projects statically, and don't know what the
 * unreleased versions are when Gradle projects are set up, so we use "build-unreleased-version-*" as placeholders
 * and configure them to build various versions here.
 */
public class InternalDistributionBwcSetupPlugin implements Plugin<Project> {

    private ProviderFactory providerFactory;

    @Inject
    public InternalDistributionBwcSetupPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        BuildParams.getBwcVersions().forPreviousUnreleased((BwcVersions.UnreleasedVersionInfo unreleasedVersion) -> {
            configureBwcProject(project.project(unreleasedVersion.gradleProjectPath), unreleasedVersion);
        });
    }

    private void configureBwcProject(Project project, BwcVersions.UnreleasedVersionInfo versionInfo) {
        Provider<BwcVersions.UnreleasedVersionInfo> versionInfoProvider = providerFactory.provider(() -> versionInfo);
        Provider<File> checkoutDir = versionInfoProvider.map(info -> new File(project.getBuildDir(), "bwc/checkout-" + info.branch));
        BwcSetupExtension bwcSetupExtension = project.getExtensions()
            .create("bwcSetup", BwcSetupExtension.class, project, versionInfoProvider, checkoutDir);
        BwcGitExtension gitExtension = project.getPlugins().apply(InternalBwcGitPlugin.class).getGitExtension();
        Provider<Version> bwcVersion = versionInfoProvider.map(info -> info.version);
        gitExtension.setBwcVersion(versionInfoProvider.map(info -> info.version));
        gitExtension.setBwcBranch(versionInfoProvider.map(info -> info.branch));
        gitExtension.setCheckoutDir(checkoutDir);

        // we want basic lifecycle tasks like `clean` here.
        project.getPlugins().apply(LifecycleBasePlugin.class);

        TaskProvider<Task> buildBwcTaskProvider = project.getTasks().register("buildBwc");
        List<DistributionProject> distributionProjects = resolveArchiveProjects(checkoutDir.get(), bwcVersion.get());

        for (DistributionProject distributionProject : distributionProjects) {
            createBuildBwcTask(
                bwcSetupExtension,
                project,
                bwcVersion,
                distributionProject.name,
                distributionProject.getProjectPath(),
                distributionProject.getDistFile(),
                buildBwcTaskProvider
            );

            registerBwcArtifacts(project, distributionProject);
        }
    }

    private void registerBwcArtifacts(Project bwcProject, DistributionProject distributionProject) {
        String projectName = distributionProject.name;
        String buildBwcTask = buildBwcTaskName(projectName);

        registerDistributionArchiveArtifact(bwcProject, distributionProject, buildBwcTask);
        if (distributionProject.getExpandedDistDirectory() != null) {
            String expandedDistConfiguration = "expanded-" + projectName;
            bwcProject.getConfigurations().create(expandedDistConfiguration);
            bwcProject.getArtifacts().add(expandedDistConfiguration, distributionProject.getExpandedDistDirectory(), artifact -> {
                artifact.setName("opensearch");
                artifact.builtBy(buildBwcTask);
                artifact.setType("directory");
            });
        }
    }

    private void registerDistributionArchiveArtifact(Project bwcProject, DistributionProject distributionProject, String buildBwcTask) {
        String artifactFileName = distributionProject.getDistFile().getName();
        String artifactName = "opensearch";

        String suffix = artifactFileName.endsWith("tar.gz") ? "tar.gz" : artifactFileName.substring(artifactFileName.length() - 3);
        int archIndex = artifactFileName.indexOf("x64");

        bwcProject.getConfigurations().create(distributionProject.name);
        bwcProject.getArtifacts().add(distributionProject.name, distributionProject.getDistFile(), artifact -> {
            artifact.setName(artifactName);
            artifact.builtBy(buildBwcTask);
            artifact.setType(suffix);

            String classifier = "";
            if (archIndex != -1) {
                int osIndex = artifactFileName.lastIndexOf('-', archIndex - 2);
                classifier = "-" + artifactFileName.substring(osIndex + 1, archIndex - 1) + "-x64";
            }
            artifact.setClassifier(classifier);
        });
    }

    private static List<DistributionProject> resolveArchiveProjects(File checkoutDir, Version bwcVersion) {
        List<String> projects = new ArrayList<>();
        // All active BWC branches publish rpm and deb packages
        projects.addAll(asList("deb", "rpm"));

        if (bwcVersion.onOrAfter("7.0.0")) { // starting with 7.0 we bundle a jdk which means we have platform-specific archives
            projects.addAll(
                asList(
                    "darwin-tar",
                    "darwin-arm64-tar",
                    "linux-tar",
                    "linux-arm64-tar",
                    "linux-ppc64le-tar",
                    "linux-s390x-tar",
                    "windows-zip"
                )
            );
        } else { // prior to 7.0 we published only a single zip and tar archives
            projects.addAll(asList("zip", "tar"));
        }

        return projects.stream().map(name -> {
            String baseDir = "distribution" + (name.endsWith("zip") || name.endsWith("tar") ? "/archives" : "/packages");
            String classifier = "";
            String extension = name;
            if (bwcVersion.onOrAfter("7.0.0")) {
                if (name.contains("zip") || name.contains("tar")) {
                    int index = name.lastIndexOf('-');
                    String baseName = name.substring(0, index);
                    classifier = "-" + baseName;
                    // The x64 variants do not have the architecture built into the task name, so it needs to be appended
                    if (name.equals("darwin-tar") || name.equals("linux-tar") || name.equals("windows-zip")) {
                        classifier += "-x64";
                    }
                    extension = name.substring(index + 1);
                    if (extension.equals("tar")) {
                        extension += ".gz";
                    }
                } else if (name.contains("deb")) {
                    classifier = "_amd64";
                } else if (name.contains("rpm")) {
                    classifier = ".x86_64";
                }
            } else {
                extension = name.substring(4);
            }
            return new DistributionProject(name, baseDir, bwcVersion, classifier, extension, checkoutDir);
        }).collect(Collectors.toList());
    }

    private static String buildBwcTaskName(String projectName) {
        return "buildBwc"
            + stream(projectName.split("-")).map(i -> i.substring(0, 1).toUpperCase(Locale.ROOT) + i.substring(1))
                .collect(Collectors.joining());
    }

    static void createBuildBwcTask(
        BwcSetupExtension bwcSetupExtension,
        Project project,
        Provider<Version> bwcVersion,
        String projectName,
        String projectPath,
        File projectArtifact,
        TaskProvider<Task> bwcTaskProvider
    ) {
        String bwcTaskName = buildBwcTaskName(projectName);
        bwcSetupExtension.bwcTask(bwcTaskName, new Action<LoggedExec>() {
            @Override
            public void execute(LoggedExec c) {
                c.getInputs().file(new File(project.getBuildDir(), "refspec"));
                c.getOutputs().files(projectArtifact);
                c.getOutputs().cacheIf("BWC distribution caching is disabled on 'master' branch", task -> {
                    String gitBranch = System.getenv("GIT_BRANCH");
                    return BuildParams.isCi() && (gitBranch == null || gitBranch.endsWith("master") == false);
                });
                c.args(projectPath.replace('/', ':') + ":assemble");
                if (project.getGradle().getStartParameter().isBuildCacheEnabled()) {
                    c.args("--build-cache");
                }
                c.doLast(new Action<Task>() {
                    @Override
                    public void execute(Task task) {
                        if (projectArtifact.exists() == false) {
                            throw new InvalidUserDataException(
                                "Building " + bwcVersion.get() + " didn't generate expected file " + projectArtifact
                            );
                        }
                    }
                });
            }
        });
        bwcTaskProvider.configure(t -> t.dependsOn(bwcTaskName));
    }

    /**
     * Represents an archive project (distribution/archives/*)
     * we build from a bwc Version in a cloned repository
     */
    private static class DistributionProject {
        private final String name;
        private String projectPath;
        private File distFile;
        private File expandedDistDir;

        DistributionProject(String name, String baseDir, Version version, String classifier, String extension, File checkoutDir) {
            this.name = name;
            this.projectPath = baseDir + "/" + name;
            if (version.onOrAfter("1.1.0")) {
                // Deb uses underscores (I don't know why...):
                // https://github.com/opensearch-project/OpenSearch/blob/f6d9a86f0e2e8241fd58b7e8b6cdeaf931b5108f/distribution/packages/build.gradle#L139
                final String separator = name.equals("deb") ? "_" : "-";
                this.distFile = new File(
                    checkoutDir,
                    baseDir
                        + "/"
                        + name
                        + "/build/distributions/opensearch-min"
                        + separator
                        + version
                        + "-SNAPSHOT"
                        + classifier
                        + "."
                        + extension
                );
            } else {
                this.distFile = new File(
                    checkoutDir,
                    baseDir + "/" + name + "/build/distributions/opensearch-" + version + "-SNAPSHOT" + classifier + "." + extension
                );
            }
            // we only ported this down to the 7.x branch.
            if (version.onOrAfter("7.10.0") && (name.endsWith("zip") || name.endsWith("tar"))) {
                this.expandedDistDir = new File(checkoutDir, baseDir + "/" + name + "/build/install");
            }
        }

        public String getProjectPath() {
            return projectPath;
        }

        public File getDistFile() {
            return distFile;
        }

        public File getExpandedDistDirectory() {
            return expandedDistDir;
        }
    }
}
