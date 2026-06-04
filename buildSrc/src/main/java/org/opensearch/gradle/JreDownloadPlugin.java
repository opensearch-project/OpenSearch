/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

public class JreDownloadPlugin implements Plugin<Project> {
    public static final String VENDOR_ADOPTIUM = "adoptium";

    private static final String REPO_NAME_PREFIX = "jre_repo_";
    private static final String EXTENSION_NAME = "jres";
    public static final String JRE_TRIMMED_PREFIX = "jdk-?\\d.*-jre";

    @Override
    public void apply(Project project) {
        Attribute<Boolean> jreAttribute = Attribute.of("jre", Boolean.class);
        project.getDependencies().getAttributesSchema().attribute(jreAttribute);
        project.getDependencies().getArtifactTypes().maybeCreate(ArtifactTypeDefinition.ZIP_TYPE);
        project.getDependencies().registerTransform(UnzipTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.ZIP_TYPE)
                .attribute(jreAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jreAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JRE_TRIMMED_PREFIX));
        });

        ArtifactTypeDefinition tarArtifactTypeDefinition = project.getDependencies().getArtifactTypes().maybeCreate("tar.gz");
        project.getDependencies().registerTransform(SymbolicLinkPreservingUntarTransform.class, transformSpec -> {
            transformSpec.getFrom()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, tarArtifactTypeDefinition.getName())
                .attribute(jreAttribute, true);
            transformSpec.getTo()
                .attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE)
                .attribute(jreAttribute, true);
            transformSpec.parameters(parameters -> parameters.setTrimmedPrefixPattern(JRE_TRIMMED_PREFIX));
        });

        NamedDomainObjectContainer<Jre> jresContainer = project.container(Jre.class, name -> {
            Configuration configuration = project.getConfigurations().create("jre_" + name);
            configuration.setCanBeConsumed(false);
            configuration.getAttributes().attribute(ArtifactAttributes.ARTIFACT_FORMAT, ArtifactTypeDefinition.DIRECTORY_TYPE);
            configuration.getAttributes().attribute(jreAttribute, true);
            Jre jre = new Jre(name, configuration, project.getObjects());
            configuration.defaultDependencies(dependencies -> {
                jre.finalizeValues();
                setupRepository(project, jre);
                dependencies.add(project.getDependencies().create(dependencyNotation(jre)));
            });
            return jre;
        });
        project.getExtensions().add(EXTENSION_NAME, jresContainer);
    }

    private void setupRepository(Project project, Jre jre) {
        RepositoryHandler repositories = project.getRepositories();

        /*
         * Define the appropriate repository for the given JRE vendor and version
         *
         * For Oracle/OpenJDK/AdoptOpenJDK we define a repository per-version.
         */
        String repoName = REPO_NAME_PREFIX + jre.getVendor() + "_" + jre.getVersion();
        String repoUrl;
        String artifactPattern;

        if (jre.getVendor().equals(VENDOR_ADOPTIUM)) {
            repoUrl = "https://github.com/adoptium/temurin" + jre.getMajor() + "-binaries/releases/download/";

            if (jre.getMajor().equals("8")) {
                // JDK-8 updates are always suffixed with 'U' (fe OpenJDK8U).
                artifactPattern = "jdk"
                    + jre.getBaseVersion()
                    + "-"
                    + jre.getBuild()
                    + "/OpenJDK"
                    + jre.getMajor()
                    + "U"
                    + "-jre_[classifier]_[module]_hotspot_"
                    + jre.getBaseVersion()
                    + jre.getBuild()
                    + ".[ext]";
            } else {
                // JDK updates are suffixed with 'U' (fe OpenJDK17U), whereas GA releases are not (fe OpenJDK17).
                // To distinguish between those, the GA releases have only major version component (fe 17+32),
                // the updates always have minor/patch components (fe 17.0.1+12), checking for the presence of
                // version separator '.' should be enough.
                artifactPattern = "jdk-" + jre.getBaseVersion() + "+" + jre.getBuild() + "/OpenJDK" + jre.getMajor()
                // JDK-20 does use 'U' suffix all the time, no matter it is update or GA release
                    + (jre.getBaseVersion().contains(".") || jre.getBaseVersion().matches("^2\\d+$") ? "U" : "")
                    + "-jre_[classifier]_[module]_hotspot_"
                    + jre.getBaseVersion()
                    + "_"
                    + jre.getBuild()
                    + ".[ext]";
            }
        } else {
            throw new GradleException("Unknown JDK vendor [" + jre.getVendor() + "]");
        }

        // Define the repository if we haven't already
        if (repositories.findByName(repoName) == null) {
            repositories.ivy(repo -> {
                repo.setName(repoName);
                repo.setUrl(repoUrl);
                repo.metadataSources(IvyArtifactRepository.MetadataSources::artifact);
                repo.patternLayout(layout -> layout.artifact(artifactPattern));
                repo.content(repositoryContentDescriptor -> repositoryContentDescriptor.includeGroup(groupName(jre)));
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static NamedDomainObjectContainer<Jre> getContainer(Project project) {
        return (NamedDomainObjectContainer<Jre>) project.getExtensions().getByName(EXTENSION_NAME);
    }

    private static String dependencyNotation(Jre jre) {
        String platformDep = jre.getPlatform().equals("darwin") || jre.getPlatform().equals("mac") ? "mac" : jre.getPlatform();
        String extension = jre.getPlatform().equals("windows") ? "zip" : "tar.gz";

        return groupName(jre) + ":" + platformDep + ":" + jre.getBaseVersion() + ":" + jre.getArchitecture() + "@" + extension;
    }

    private static String groupName(Jre jre) {
        return jre.getVendor() + "_" + jre.getMajor() + "_jre";
    }

}
