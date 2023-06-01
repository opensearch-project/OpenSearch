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

package org.opensearch.gradle;

import org.opensearch.gradle.transform.SymbolicLinkPreservingUntarTransform;
import org.opensearch.gradle.transform.UnzipTransform;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.RepositoryHandler;
import org.gradle.api.artifacts.repositories.IvyArtifactRepository;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.attributes.Attribute;
import org.gradle.api.internal.artifacts.ArtifactAttributes;

public class JdkDownloadPlugin implements Plugin<Project> {
    public static final String VENDOR_ADOPTIUM = "adoptium";
    public static final String VENDOR_ADOPTOPENJDK = "adoptopenjdk";
    public static final String VENDOR_OPENJDK = "openjdk";

    private static final String REPO_NAME_PREFIX = "jdk_repo_";
    private static final String EXTENSION_NAME = "jdks";
    public static final String JDK_TRIMMED_PREFIX = "jdk-?\\d.*";

    @Override
    public void apply(Project project) {
        Attribute<Boolean> jdkAttribute = Attribute.of("jdk", Boolean.class);
        project.getDependencies().getAttributesSchema().attribute(jdkAttribute);
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.ZIP_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JDK_TRIMMED_PREFIX));
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName())
                .attribute(jdkAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jdkAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JDK_TRIMMED_PREFIX));
        });

        NamedDomainObjectContainer<Jdk> jdksContainer = project.container(Jdk.class, name -> {
            Configuration configuration = project.getConfigurations().create("jdk_" + name);
            configuration.setCanBeConsumed(false);
            configuration.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
            configuration.getAttributes().attribute(jdkAttribute, true);
            Jdk jdk = new Jdk(name, configuration, project.getObjects());
            configuration.defaultDependencies(dependencies -> {
                jdk.finalizeValues();
                setupRepository(project, jdk);
                dependencies.add(project.getDependencies().create(dependencyNotation(jdk)));
            });
            return jdk;
        });
        project.getExtensions().add(EXTENSION_NAME, jdksContainer);
    }

    private void setupRepository(Project project, Jdk jdk) {
        RepositoryHandler repositories = project.getRepositories();

        /*
         * Define the appropriate repository for the given JDK vendor and version
         *
         * For Oracle/OpenJDK/AdoptOpenJDK we define a repository per-version.
         */
        String repoName = REPO_NAME_PREFIX + jdk.getVendor() + "_" + jdk.getVersion();
        String repoUrl;
        String artifactPattern;

        if (jdk.getVendor().equals(VENDOR_ADOPTIUM)) {
            repoUrl = "https://github.com/adoptium/temurin" + jdk.getMajor() + "-binaries/releases/download/";

            if (jdk.getMajor().equals("8")) {
                // JDK-8 updates are always suffixed with 'U' (fe OpenJDK8U).
                artifactPattern = "jdk"
                    + jdk.getBaseVersion()
                    + "-"
                    + jdk.getBuild()
                    + "/OpenJDK"
                    + jdk.getMajor()
                    + "U"
                    + "-jdk_[classifier]_[module]_hotspot_"
                    + jdk.getBaseVersion()
                    + jdk.getBuild()
                    + ".[ext]";
            } else {
                // JDK updates are suffixed with 'U' (fe OpenJDK17U), whereas GA releases are not (fe OpenJDK17).
                // To distinguish between those, the GA releases have only major version component (fe 17+32),
                // the updates always have minor/patch components (fe 17.0.1+12), checking for the presence of
                // version separator '.' should be enough.
                artifactPattern = "jdk-" + jdk.getBaseVersion() + "+" + jdk.getBuild() + "/OpenJDK" + jdk.getMajor()
                // JDK-20 does use 'U' suffix all the time, no matter it is update or GA release
                    + (jdk.getBaseVersion().contains(".") || jdk.getBaseVersion().matches("^2\\d+$") ? "U" : "")
                    + "-jdk_[classifier]_[module]_hotspot_"
                    + jdk.getBaseVersion()
                    + "_"
                    + jdk.getBuild()
                    + ".[ext]";
            }
        } else if (jdk.getVendor().equals(VENDOR_ADOPTOPENJDK)) {
            repoUrl = "https://api.adoptopenjdk.net/v3/binary/version/";
            if (jdk.getMajor().equals("8")) {
                // legacy pattern for JDK 8
                artifactPattern = "jdk"
                    + jdk.getBaseVersion()
                    + "-"
                    + jdk.getBuild()
                    + "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
            } else {
                // current pattern since JDK 9
                artifactPattern = "jdk-"
                    + jdk.getBaseVersion()
                    + "+"
                    + jdk.getBuild()
                    + "/[module]/[classifier]/jdk/hotspot/normal/adoptopenjdk";
            }
        } else if (jdk.getVendor().equals(VENDOR_OPENJDK)) {
            repoUrl = "https://download.oracle.com";
            if (jdk.getHash() != null) {
                // current pattern since 12.0.1
                artifactPattern = "java/GA/jdk"
                    + jdk.getBaseVersion()
                    + "/"
                    + jdk.getHash()
                    + "/"
                    + jdk.getBuild()
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            } else {
                // simpler legacy pattern from JDK 9 to JDK 12 that we are advocating to Oracle to bring back
                artifactPattern = "java/GA/jdk"
                    + jdk.getMajor()
                    + "/"
                    + jdk.getBuild()
                    + "/GPL/openjdk-[revision]_[module]-[classifier]_bin.[ext]";
            }
        } else {
            throw new GradleException("Unknown JDK vendor [" + jdk.getVendor() + "]");
        }

        // Define the repository if we haven't already
        if (repositories.findByName(repoName) == null) {
            repositories.ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(repoUrl);
                repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                repo.patternLayout(layout -> layout.artifact(artifactPattern));
                repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeGroup(groupName(jdk)));
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<Jdk> getContainer(Project project) {
        return (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static String dependencyNotation(Jdk jdk) {
        String platformDep = jdk.getPlatform().equals("darwin") || jdk.getPlatform().equals("mac")
            ? (jdk.getVendor().equals(VENDOR_OPENJDK) ? "osx" : "mac")
            : jdk.getPlatform();
        String extension = jdk.getPlatform().equals("windows") ? "zip" : "tar.gz";

        return groupName(jdk) + ":" + platformDep + ":" + jdk.getBaseVersion() + ":" + jdk.getArchitecture() + "@" + extension;
    }

    private static String groupName(Jdk jdk) {
        return jdk.getVendor() + "_" + jdk.getMajor();
    }

}
