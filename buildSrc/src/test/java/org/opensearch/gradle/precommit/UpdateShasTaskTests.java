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

import org.apache.commons.io.FileUtils;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class UpdateShasTaskTests extends GradleUnitTestCase {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private UpdateShasTask task;

    private Project project;

    private Dependency dependency;

    @Before
    public void prepare() throws IOException {
        project = createProject();
        task = createUpdateShasTask(project);
        dependency = project.getDependencies().localGroovy();

    }

    @Test
    public void whenDependencyDoesntExistThenShouldDeleteDependencySha() throws IOException, NoSuchAlgorithmException {

        File unusedSha = createFileIn(getLicensesDir(project), "test.sha1", "");
        task.updateShas();

        assertFalse(unusedSha.exists());
    }

    @Test
    public void whenDependencyExistsButShaNotThenShouldCreateNewShaFile() throws IOException, NoSuchAlgorithmException {
        project.getDependencies().add("someCompileConfiguration", dependency);

        getLicensesDir(project).mkdir();
        task.updateShas();

        assertTrue(
            "Expected a sha file to exist with a name prefix of 'groovy-",
            Files.list(getLicensesDir(project).toPath()).anyMatch(sha -> sha.toFile().getName().startsWith("groovy-"))
        );
    }

    @Test
    public void whenDependencyAndWrongShaExistsThenShouldNotOverwriteShaFile() throws IOException, NoSuchAlgorithmException {
        project.getDependencies().add("someCompileConfiguration", dependency);

        File groovyJar = task.getParentTask().getDependencies().get().getFiles().iterator().next();
        String groovyShaName = groovyJar.getName() + ".sha1";

        File groovySha = createFileIn(getLicensesDir(project), groovyShaName, "content");
        task.updateShas();

        assertThat(FileUtils.readFileToString(groovySha), equalTo("content"));
    }

    @Test
    public void whenLicensesDirDoesntExistThenShouldThrowException() throws IOException, NoSuchAlgorithmException {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("isn't a valid directory"));

        task.updateShas();
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);

        Configuration compileClasspath = project.getConfigurations().getByName("compileClasspath");
        Configuration someCompileConfiguration = project.getConfigurations().create("someCompileConfiguration");
        // Declare a configuration that is going to resolve the compile classpath of the application
        project.getConfigurations().add(compileClasspath.extendsFrom(someCompileConfiguration));

        return project;
    }

    private File getLicensesDir(Project project) {
        return getFile(project, "licenses");
    }

    private File getFile(Project project, String fileName) {
        return project.getProjectDir().toPath().resolve(fileName).toFile();
    }

    private File createFileIn(File parent, String name, String content) throws IOException {
        parent.mkdir();

        Path path = parent.toPath().resolve(name);
        File file = path.toFile();

        Files.write(path, content.getBytes(), StandardOpenOption.CREATE);

        return file;
    }

    private UpdateShasTask createUpdateShasTask(Project project) {
        UpdateShasTask task = project.getTasks().register("updateShas", UpdateShasTask.class).get();

        task.setParentTask(createDependencyLicensesTask(project));
        return task;
    }

    private TaskProvider<DependencyLicensesTask> createDependencyLicensesTask(Project project) {
        TaskProvider<DependencyLicensesTask> task = project.getTasks()
            .register("dependencyLicenses", DependencyLicensesTask.class, new Action<DependencyLicensesTask>() {
                @Override
                public void execute(DependencyLicensesTask dependencyLicensesTask) {
                    dependencyLicensesTask.getDependencies().set(getDependencies(project));
                }
            });

        return task;
    }

    private FileCollection getDependencies(Project project) {
        return project.getConfigurations().getByName("compileClasspath");
    }
}
