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

import org.opensearch.gradle.LoggedExec;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;

import java.io.File;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public class LoggerUsageTask extends PrecommitTask {

    private FileCollection classpath;

    public LoggerUsageTask() {
        setDescription("Runs LoggerUsageCheck on output directories of all source sets");
    }

    @TaskAction
    public void runLoggerUsageTask() {
        LoggedExec.javaexec(getProject(), spec -> {
            spec.getMainClass().set("org.opensearch.test.loggerusage.OpenSearchLoggerUsageChecker");
            spec.classpath(getClasspath());
            getClassDirectories().forEach(spec::args);
        });
    }

    @Classpath
    public FileCollection getClasspath() {
        return classpath;
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    public FileCollection getClassDirectories() {
        return getProject().getConvention()
            .getPlugin(JavaPluginConvention.class)
            .getSourceSets()
            .stream()
            // Don't pick up all source sets like the java9 ones as logger-check doesn't support the class format
            .filter(
                sourceSet -> sourceSet.getName().equals(SourceSet.MAIN_SOURCE_SET_NAME)
                    || sourceSet.getName().equals(SourceSet.TEST_SOURCE_SET_NAME)
            )
            .map(sourceSet -> sourceSet.getOutput().getClassesDirs())
            .reduce(FileCollection::plus)
            .orElse(getProject().files())
            .filter(File::exists);
    }

}
