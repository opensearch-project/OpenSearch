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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;

public class DependencyLicensesTaskTests extends GradleUnitTestCase {

    private static final String PERMISSIVE_LICENSE_TEXT = "Eclipse Public License - v 2.0";
    private static final String STRICT_LICENSE_TEXT = "GNU LESSER GENERAL PUBLIC LICENSE Version 3";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private UpdateShasTask updateShas;

    private TaskProvider<DependencyLicensesTask> task;

    private Project project;

    private Dependency dependency;

    @Before
    public void prepare() {
        project = createProject();
        task = createDependencyLicensesTask(project);
        updateShas = createUpdateShasTask(project, task);
        dependency = project.getDependencies().localGroovy();
    }

    @Test
    public void givenProjectWithLicensesDirButNoDependenciesThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("exists, but there are no dependencies"));

        getLicensesDir(project).mkdir();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithoutLicensesDirButWithDependenciesThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("does not exist, but there are dependencies"));

        project.getDependencies().add("someCompileConfiguration", dependency);
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithoutLicensesDirNorDependenciesThenShouldReturnSilently() throws Exception {
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyButNoShaFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing SHA for "));

        File licensesDir = getLicensesDir(project);
        createFileIn(licensesDir, "groovy-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);
        createFileIn(licensesDir, "groovy-NOTICE.txt", "");

        project.getDependencies().add("someCompileConfiguration", project.getDependencies().localGroovy());
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyButNoLicenseFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing LICENSE for "));

        project.getDependencies().add("someCompileConfiguration", project.getDependencies().localGroovy());

        getLicensesDir(project).mkdir();
        updateShas.updateShas();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyButNoNoticeFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing NOTICE for "));

        project.getDependencies().add("someCompileConfiguration", dependency);

        createFileIn(getLicensesDir(project), "groovy-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);

        updateShas.updateShas();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithStrictDependencyButNoSourcesFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing SOURCES for "));

        project.getDependencies().add("someCompileConfiguration", dependency);

        createFileIn(getLicensesDir(project), "groovy-LICENSE.txt", STRICT_LICENSE_TEXT);
        createFileIn(getLicensesDir(project), "groovy-NOTICE.txt", "");

        updateShas.updateShas();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithStrictDependencyAndEverythingInOrderThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("someCompileConfiguration", dependency);

        createFileIn(getLicensesDir(project), "groovy-LICENSE.txt", STRICT_LICENSE_TEXT);
        createFileIn(getLicensesDir(project), "groovy-NOTICE.txt", "");
        createFileIn(getLicensesDir(project), "groovy-SOURCES.txt", "");

        createFileIn(getLicensesDir(project), "javaparser-core-LICENSE.txt", STRICT_LICENSE_TEXT);
        createFileIn(getLicensesDir(project), "javaparser-core-NOTICE.txt", "");
        createFileIn(getLicensesDir(project), "javaparser-core-SOURCES.txt", "");

        updateShas.updateShas();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyAndEverythingInOrderThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);

        createAllDefaultDependencyFiles(licensesDir, "groovy", "javaparser-core");
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithALicenseButWithoutTheDependencyThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Unused license "));

        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy", "javaparser-core");
        createFileIn(licensesDir, "non-declared-LICENSE.txt", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithANoticeButWithoutTheDependencyThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Unused notice "));

        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy", "javaparser-core");
        createFileIn(licensesDir, "non-declared-NOTICE.txt", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithAShaButWithoutTheDependencyThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Unused sha files found: \n"));

        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy", "javaparser-core");
        createFileIn(licensesDir, "non-declared.sha1", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithADependencyWithWrongShaThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("SHA has changed! Expected "));

        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy");

        Path groovySha = Files.list(licensesDir.toPath()).filter(file -> file.toFile().getName().contains("sha")).findFirst().get();

        Files.write(groovySha, new byte[] { 1 }, StandardOpenOption.CREATE);

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithADependencyMappingThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy", "javaparser");

        Map<String, String> mappings = new HashMap<>();
        mappings.put("from", "javaparser-core");
        mappings.put("to", "javaparser");

        task.get().mapping(mappings);
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithAIgnoreShaConfigurationAndNoShaFileThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("someCompileConfiguration", dependency);

        File licensesDir = getLicensesDir(project);
        createFileIn(licensesDir, "groovy-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);
        createFileIn(licensesDir, "groovy-NOTICE.txt", "");

        createFileIn(licensesDir, "javaparser-core-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);
        createFileIn(licensesDir, "javaparser-core-NOTICE.txt", "");

        task.get().ignoreSha("groovy");
        task.get().ignoreSha("groovy-ant");
        task.get().ignoreSha("groovy-astbuilder");
        task.get().ignoreSha("groovy-console");
        task.get().ignoreSha("groovy-datetime");
        task.get().ignoreSha("groovy-dateutil");
        task.get().ignoreSha("groovy-groovydoc");
        task.get().ignoreSha("groovy-json");
        task.get().ignoreSha("groovy-nio");
        task.get().ignoreSha("groovy-sql");
        task.get().ignoreSha("groovy-templates");
        task.get().ignoreSha("groovy-test");
        task.get().ignoreSha("groovy-xml");
        task.get().ignoreSha("javaparser-core");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithoutLicensesDirWhenAskingForShaFilesThenShouldThrowException() {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("isn't a valid directory"));

        task.get().getShaFiles();
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

    private void createAllDefaultDependencyFiles(File licensesDir, String... dependencyNames) throws IOException, NoSuchAlgorithmException {
        for (final String dependencyName : dependencyNames) {
            createFileIn(licensesDir, dependencyName + "-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);
            createFileIn(licensesDir, dependencyName + "-NOTICE.txt", "");
        }

        updateShas.updateShas();
    }

    private File getLicensesDir(Project project) {
        return getFile(project, "licenses");
    }

    private File getFile(Project project, String fileName) {
        return project.getProjectDir().toPath().resolve(fileName).toFile();
    }

    private void createFileIn(File parent, String name, String content) throws IOException {
        parent.mkdir();

        Path file = parent.toPath().resolve(name);
        file.toFile().createNewFile();

        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    }

    private UpdateShasTask createUpdateShasTask(Project project, TaskProvider<DependencyLicensesTask> dependencyLicensesTask) {
        UpdateShasTask task = project.getTasks().register("updateShas", UpdateShasTask.class).get();

        task.setParentTask(dependencyLicensesTask);
        return task;
    }

    private TaskProvider<DependencyLicensesTask> createDependencyLicensesTask(Project project) {
        TaskProvider<DependencyLicensesTask> task = project.getTasks()
            .register("dependencyLicenses", DependencyLicensesTask.class, new Action<DependencyLicensesTask>() {
                @Override
                public void execute(DependencyLicensesTask dependencyLicensesTask) {
                    dependencyLicensesTask.getDependencies().set(getDependencies(project));

                    final Map<String, String> mappings = new HashMap<>();
                    mappings.put("from", "groovy-.*");
                    mappings.put("to", "groovy");

                    dependencyLicensesTask.mapping(mappings);
                }
            });

        return task;
    }

    private FileCollection getDependencies(Project project) {
        return project.getConfigurations().getByName("compileClasspath");
    }
}
