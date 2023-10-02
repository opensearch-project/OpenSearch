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

import org.opensearch.gradle.VersionProperties;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.TaskProvider;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.opensearch.gradle.util.Util.capitalize;

public class InternalDistributionArchiveCheckPlugin implements Plugin<Project> {

    private ArchiveOperations archiveOperations;

    @Inject
    public InternalDistributionArchiveCheckPlugin(ArchiveOperations archiveOperations) {
        this.archiveOperations = archiveOperations;
    }

    @Override
    public void apply(Project project) {
        project.getPlugins().apply(BasePlugin.class);
        String buildTaskName = calculateBuildTask(project.getName());
        TaskProvider<Task> buildDistTask = project.getParent().getTasks().named(buildTaskName);
        DistributionArchiveCheckExtension distributionArchiveCheckExtension = project.getExtensions()
            .create("distributionArchiveCheck", DistributionArchiveCheckExtension.class);

        File archiveExtractionDir = calculateArchiveExtractionDir(project);
        // sanity checks if archives can be extracted
        TaskProvider<Copy> checkExtraction = registerCheckExtractionTask(project, buildDistTask, archiveExtractionDir);
        checkExtraction.configure(InternalDistributionArchiveSetupPlugin.configure(buildTaskName));
        TaskProvider<Task> checkLicense = registerCheckLicenseTask(project, checkExtraction);
        checkLicense.configure(InternalDistributionArchiveSetupPlugin.configure(buildTaskName));

        TaskProvider<Task> checkNotice = registerCheckNoticeTask(project, checkExtraction);
        checkNotice.configure(InternalDistributionArchiveSetupPlugin.configure(buildTaskName));
        TaskProvider<Task> checkTask = project.getTasks().named("check");
        checkTask.configure(task -> {
            task.dependsOn(checkExtraction);
            task.dependsOn(checkLicense);
            task.dependsOn(checkNotice);
        });
    }

    private File calculateArchiveExtractionDir(Project project) {
        if (project.getName().contains("tar")) {
            return new File(project.getBuildDir(), "tar-extracted");
        }
        if (project.getName().contains("zip") == false) {
            throw new GradleException("Expecting project name containing 'zip' or 'tar'.");
        }
        return new File(project.getBuildDir(), "zip-extracted");
    }

    private TaskProvider<Task> registerCheckNoticeTask(Project project, TaskProvider<Copy> checkExtraction) {
        return project.getTasks().register("checkNotice", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    final List<String> noticeLines = Arrays.asList(
                        "OpenSearch (https://opensearch.org/)",
                        "Copyright OpenSearch Contributors"
                    );
                    final Path noticePath = checkExtraction.get()
                        .getDestinationDir()
                        .toPath()
                        .resolve("opensearch-" + VersionProperties.getOpenSearch() + "/NOTICE.txt");
                    assertLinesInFile(noticePath, noticeLines);
                }
            });
        });
    }

    private TaskProvider<Task> registerCheckLicenseTask(Project project, TaskProvider<Copy> checkExtraction) {
        return project.getTasks().register("checkLicense", task -> {
            task.dependsOn(checkExtraction);
            task.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    String licenseFilename = "APACHE-LICENSE-2.0.txt";
                    final List<String> licenseLines;
                    try {
                        licenseLines = Files.readAllLines(project.getRootDir().toPath().resolve("licenses/" + licenseFilename));
                        final Path licensePath = checkExtraction.get()
                            .getDestinationDir()
                            .toPath()
                            .resolve("opensearch-" + VersionProperties.getOpenSearch() + "/LICENSE.txt");
                        assertLinesInFile(licensePath, licenseLines);
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                }
            });
        });
    }

    private TaskProvider<Copy> registerCheckExtractionTask(Project project, TaskProvider<Task> buildDistTask, File archiveExtractionDir) {
        return project.getTasks().register("checkExtraction", Copy.class, t -> {
            t.dependsOn(buildDistTask);
            if (project.getName().contains("tar")) {
                t.from(archiveOperations.tarTree(distTaskOutput(buildDistTask)));
            } else {
                t.from(archiveOperations.zipTree(distTaskOutput(buildDistTask)));
            }
            t.into(archiveExtractionDir);
            // common sanity checks on extracted archive directly as part of checkExtraction
            t.eachFile(fileCopyDetails -> assertNoClassFile(fileCopyDetails.getFile()));
        });
    }

    private static void assertLinesInFile(Path path, List<String> expectedLines) {
        try {
            final List<String> actualLines = Files.readAllLines(path);
            int line = 0;
            for (final String expectedLine : expectedLines) {
                final String actualLine = actualLines.get(line);
                if (expectedLine.equals(actualLine) == false) {
                    throw new GradleException(
                        "expected line [" + (line + 1) + "] in [" + path + "] to be [" + expectedLine + "] but was [" + actualLine + "]"
                    );
                }
                line++;
            }
        } catch (IOException ioException) {
            throw new GradleException("Unable to read from file " + path, ioException);
        }
    }

    private static void assertNoClassFile(File file) {
        if (file.getName().endsWith(".class")) {
            throw new GradleException("Detected class file in distribution ('" + file.getName() + "')");
        }
    }

    private Object distTaskOutput(TaskProvider<Task> buildDistTask) {
        return new Callable<File>() {
            @Override
            public File call() {
                return buildDistTask.get().getOutputs().getFiles().getSingleFile();
            }

            @Override
            public String toString() {
                return call().getAbsolutePath();
            }
        };
    }

    private String calculateBuildTask(String projectName) {
        return "build" + Arrays.stream(projectName.split("-")).map(f -> capitalize(f)).collect(Collectors.joining());
    }

}
