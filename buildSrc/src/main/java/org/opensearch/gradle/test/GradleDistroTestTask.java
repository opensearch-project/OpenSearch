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

import org.opensearch.gradle.vagrant.VagrantMachine;
import org.opensearch.gradle.vagrant.VagrantShellTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.options.Option;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Run a gradle task of the current build, within the configured vagrant VM.
 */
public class GradleDistroTestTask extends VagrantShellTask {

    private String taskName;
    private String testClass;
    private List<String> extraArgs = new ArrayList<>();
    private final Project project;

    @Inject
    public GradleDistroTestTask(Project project) {
        super(project);
        this.project = project;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    @Input
    public String getTaskName() {
        return taskName;
    }

    @Option(option = "tests", description = "Sets test class or method name to be included, '*' is supported.")
    public void setTestClass(String testClass) {
        this.testClass = testClass;
    }

    @Input
    public List<String> getExtraArgs() {
        return extraArgs;
    }

    public void extraArg(String arg) {
        this.extraArgs.add(arg);
    }

    @Override
    protected List<String> getWindowsScript() {
        return getScript(true);
    }

    @Override
    protected List<String> getLinuxScript() {
        return getScript(false);
    }

    private List<String> getScript(boolean isWindows) {
        String cacheDir = project.getBuildDir() + "/gradle-cache";
        StringBuilder line = new StringBuilder();
        line.append(isWindows ? "& .\\gradlew " : "./gradlew ");
        line.append(taskName);
        line.append(" --project-cache-dir ");
        line.append(isWindows ? VagrantMachine.convertWindowsPath(project, cacheDir) : VagrantMachine.convertLinuxPath(project, cacheDir));
        line.append(" -S");
        line.append(" --parallel");
        line.append(" -D'org.gradle.logging.level'=" + project.getGradle().getStartParameter().getLogLevel());
        if (testClass != null) {
            line.append(" --tests=");
            line.append(testClass);
        }
        extraArgs.stream().map(s -> " " + s).forEach(line::append);
        return Collections.singletonList(line.toString());
    }
}
