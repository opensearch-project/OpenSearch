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

import org.opensearch.gradle.Architecture;
import org.opensearch.gradle.BwcVersions;
import org.opensearch.gradle.DistributionDependency;
import org.opensearch.gradle.DistributionDownloadPlugin;
import org.opensearch.gradle.DistributionResolution;
import org.opensearch.gradle.JavaPackageType;
import org.opensearch.gradle.OpenSearchDistribution;
import org.opensearch.gradle.Version;
import org.opensearch.gradle.VersionProperties;
import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.info.GlobalBuildInfoPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;

import java.util.function.Function;

import static org.opensearch.gradle.util.GradleUtils.projectDependency;

/**
 * An internal opensearch build plugin that registers additional
 * distribution resolution strategies to the 'opensearch.download-distribution' plugin
 * to resolve distributions from a local snapshot or a locally built bwc snapshot.
 */
public class InternalDistributionDownloadPlugin implements Plugin<Project> {

    private BwcVersions bwcVersions = null;

    @Override
    public void apply(Project project) {
        // this is needed for isInternal
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        if (!BuildParams.isInternal()) {
            throw new GradleException(
                "Plugin 'opensearch.internal-distribution-download' is not supported. "
                    + "Use 'opensearch.distribution-download' plugin instead."
            );
        }
        project.getPluginManager().apply(DistributionDownloadPlugin.class);
        this.bwcVersions = BuildParams.getBwcVersions();
        registerInternalDistributionResolutions(DistributionDownloadPlugin.getRegistrationsContainer(project));
    }

    /**
     * Registers internal distribution resolutions.
     * <p>
     * OpenSearch distributions are resolved as project dependencies either representing
     * the current version pointing to a project either under `:distribution:archives` or :distribution:packages`.
     * <p>
     * BWC versions are resolved as project to projects under `:distribution:bwc`.
     */
    private void registerInternalDistributionResolutions(NamedDomainObjectContainer<DistributionResolution> resolutions) {

        resolutions.register("localBuild", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            if (VersionProperties.getOpenSearch().equals(distribution.getVersion())) {
                // non-external project, so depend on local build
                return new ProjectBasedDistributionDependency(
                    config -> projectDependency(project, distributionProjectPath(distribution), config)
                );
            }
            return null;
        }));

        resolutions.register("bwc", distributionResolution -> distributionResolution.setResolver((project, distribution) -> {
            BwcVersions.UnreleasedVersionInfo unreleasedInfo = bwcVersions.unreleasedInfo(Version.fromString(distribution.getVersion()));
            if (unreleasedInfo != null) {
                if (distribution.getBundledJdk() == JavaPackageType.NONE) {
                    throw new GradleException(
                        "Configuring a snapshot bwc distribution ('"
                            + distribution.getName()
                            + "') "
                            + "without a bundled JDK is not supported."
                    );
                }
                String projectConfig = getProjectConfig(distribution, unreleasedInfo);
                return new ProjectBasedDistributionDependency(
                    (config) -> projectDependency(project, unreleasedInfo.gradleProjectPath, projectConfig)
                );
            }
            return null;
        }));
    }

    /**
     * Will be removed once this is backported to all unreleased branches.
     */
    private static String getProjectConfig(OpenSearchDistribution distribution, BwcVersions.UnreleasedVersionInfo info) {
        String distributionProjectName = distributionProjectName(distribution);
        if (distribution.getType().shouldExtract()) {
            return (info.gradleProjectPath.equals(":distribution") || info.version.before("7.10.0"))
                ? distributionProjectName
                : "expanded-" + distributionProjectName;
        } else {
            return distributionProjectName;

        }

    }

    private static String distributionProjectPath(OpenSearchDistribution distribution) {
        String projectPath = ":distribution";
        switch (distribution.getType()) {
            case INTEG_TEST_ZIP:
                projectPath += ":archives:integ-test-zip";
                break;

            case DOCKER:
                projectPath += ":docker:";
                projectPath += distributionProjectName(distribution);
                break;

            default:
                projectPath += distribution.getType() == OpenSearchDistribution.Type.ARCHIVE ? ":archives:" : ":packages:";
                projectPath += distributionProjectName(distribution);
                break;
        }
        return projectPath;
    }

    /**
     * Works out the gradle project name that provides a distribution artifact.
     *
     * @param distribution the distribution from which to derive a project name
     * @return the name of a project. It is not the full project path, only the name.
     */
    private static String distributionProjectName(OpenSearchDistribution distribution) {
        OpenSearchDistribution.Platform platform = distribution.getPlatform();
        Architecture architecture = distribution.getArchitecture();
        String projectName = "";

        final String archString = platform == OpenSearchDistribution.Platform.WINDOWS || architecture == Architecture.X64
            ? ""
            : "-" + architecture.toString().toLowerCase();

        if (distribution.getBundledJdk() == JavaPackageType.NONE) {
            projectName += "no-jdk-";
        } else if (distribution.getBundledJdk() == JavaPackageType.JRE) {
            projectName += "jre-";
        }
        switch (distribution.getType()) {
            case ARCHIVE:
                if (Version.fromString(distribution.getVersion()).onOrAfter("7.0.0")) {
                    projectName += platform.toString() + archString + (platform == OpenSearchDistribution.Platform.WINDOWS
                        ? "-zip"
                        : "-tar");
                } else {
                    projectName = "zip";
                }
                break;

            case DOCKER:
                projectName += "docker" + archString + "-export";
                break;

            default:
                projectName += distribution.getType();
                break;
        }
        return projectName;
    }

    private static class ProjectBasedDistributionDependency implements DistributionDependency {

        private Function<String, Dependency> function;

        ProjectBasedDistributionDependency(Function<String, Dependency> function) {
            this.function = function;
        }

        @Override
        public Object getDefaultNotation() {
            return function.apply("default");
        }

        @Override
        public Object getExtractedNotation() {
            return function.apply("extracted");
        }
    }
}
