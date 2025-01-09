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

package org.opensearch.gradle.precommit;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checks source files for correct file permissions.
 */
public class FilePermissionsTask extends DefaultTask {

    /**
     * A pattern set of which files should be checked.
     */
    private final PatternFilterable filesFilter = new PatternSet()
        // we always include all source files, and exclude what should not be checked
        .include("**")
        // exclude sh files that might have the executable bit set
        .exclude("**/*.sh");

    private final File outputMarker;
    private final Project project;

    @Inject
    public FilePermissionsTask(Project project) {
        setDescription("Checks java source files for correct file permissions");
        this.project = project;
        this.outputMarker = new File(project.getBuildDir(), "markers/filePermissions");
    }

    private static boolean isExecutableFile(File file) {
        try {
            Set<PosixFilePermission> permissions = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class)
                .readAttributes()
                .permissions();
            return permissions.contains(PosixFilePermission.OTHERS_EXECUTE)
                || permissions.contains(PosixFilePermission.OWNER_EXECUTE)
                || permissions.contains(PosixFilePermission.GROUP_EXECUTE);
        } catch (IOException e) {
            throw new IllegalStateException("unable to read the file " + file + " attributes", e);
        }
    }

    /**
     * Returns the files this task will check
     */
    @InputFiles
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getFiles() {
        return GradleUtils.getJavaSourceSets(project)
            .stream()
            .map(sourceSet -> sourceSet.getAllSource().matching(filesFilter))
            .reduce(FileTree::plus)
            .orElse(project.files().getAsFileTree());
    }

    @TaskAction
    public void checkInvalidPermissions() throws IOException {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            throw new StopExecutionException();
        }
        List<String> failures = getFiles().getFiles()
            .stream()
            .filter(FilePermissionsTask::isExecutableFile)
            .map(file -> "Source file is executable: " + file)
            .collect(Collectors.toList());

        if (!failures.isEmpty()) {
            throw new GradleException("Found invalid file permissions:\n" + String.join("\n", failures));
        }

        outputMarker.getParentFile().mkdirs();
        Files.write(outputMarker.toPath(), "done".getBytes("UTF-8"));
    }

    @OutputFile
    public File getOutputMarker() {
        return outputMarker;
    }

}
