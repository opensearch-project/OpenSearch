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

package org.opensearch.gradle.test.rest;

import org.opensearch.gradle.VersionProperties;
import org.opensearch.gradle.info.BuildParams;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import javax.inject.Inject;
import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Copies the Rest YAML test to the current projects test resources output directory.
 * This is intended to be be used from {@link RestResourcesPlugin} since the plugin wires up the needed
 * configurations and custom extensions.
 * @see RestResourcesPlugin
 */
public class CopyRestTestsTask extends DefaultTask {
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";
    final ListProperty<String> includeCore = getProject().getObjects().listProperty(String.class);

    String sourceSetName;
    Configuration coreConfig;
    Configuration additionalConfig;

    private final PatternFilterable corePatternSet;

    public CopyRestTestsTask() {
        corePatternSet = getPatternSetFactory().create();
    }

    @Inject
    protected Factory<PatternSet> getPatternSetFactory() {
        throw new UnsupportedOperationException();
    }

    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @Inject
    protected ArchiveOperations getArchiveOperations() {
        throw new UnsupportedOperationException();
    }

    @Input
    public ListProperty<String> getIncludeCore() {
        return includeCore;
    }

    @Input
    String getSourceSetName() {
        return sourceSetName;
    }

    @IgnoreEmptyDirectories
    @SkipWhenEmpty
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileTree getInputDir() {
        FileTree coreFileTree = null;
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                corePatternSet.setIncludes(includeCore.get().stream().map(prefix -> prefix + "*/**").collect(Collectors.toList()));
                coreFileTree = coreConfig.getAsFileTree().matching(corePatternSet); // directory on disk
            } else {
                coreFileTree = coreConfig.getAsFileTree(); // jar file
            }
        }
        ConfigurableFileCollection fileCollection = additionalConfig == null
            ? getProject().files(coreFileTree)
            : getProject().files(coreFileTree, additionalConfig.getAsFileTree());

        // copy tests only if explicitly requested
        return includeCore.get().isEmpty() == false || additionalConfig != null ? fileCollection.getAsFileTree() : null;
    }

    @OutputDirectory
    public File getOutputDir() {
        return new File(
            getSourceSet().orElseThrow(() -> new IllegalArgumentException("could not find source set [" + sourceSetName + "]"))
                .getOutput()
                .getResourcesDir(),
            REST_TEST_PREFIX
        );
    }

    @TaskAction
    void copy() {
        String projectPath = GradleUtils.getProjectPathFromTask(getPath());
        // only copy core tests if explicitly instructed
        if (includeCore.get().isEmpty() == false) {
            if (BuildParams.isInternal()) {
                getLogger().debug("Rest tests for project [{}] will be copied to the test resources.", projectPath);
                getFileSystemOperations().copy(c -> {
                    c.from(coreConfig.getAsFileTree());
                    c.into(getOutputDir());
                    c.include(corePatternSet.getIncludes());
                });
            } else {
                getLogger().debug(
                    "Rest tests for project [{}] will be copied to the test resources from the published jar (version: [{}]).",
                    projectPath,
                    VersionProperties.getOpenSearch()
                );
                getFileSystemOperations().copy(c -> {
                    c.from(getArchiveOperations().zipTree(coreConfig.getSingleFile()));
                    // this ends up as the same dir as outputDir
                    c.into(Objects.requireNonNull(getSourceSet().get().getOutput().getResourcesDir()));
                    c.include(
                        includeCore.get().stream().map(prefix -> REST_TEST_PREFIX + "/" + prefix + "*/**").collect(Collectors.toList())
                    );
                });
            }
        }
        // copy any additional config
        if (additionalConfig != null) {
            getFileSystemOperations().copy(c -> {
                c.from(additionalConfig.getAsFileTree());
                c.into(getOutputDir());
            });
        }
    }

    private Optional<SourceSet> getSourceSet() {
        Project project = getProject();
        return project.getExtensions().findByType(JavaPluginExtension.class) == null
            ? Optional.empty()
            : Optional.ofNullable(GradleUtils.getJavaSourceSets(project).findByName(getSourceSetName()));
    }
}
