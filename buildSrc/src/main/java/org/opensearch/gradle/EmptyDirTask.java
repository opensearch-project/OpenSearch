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

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.file.Chmod;

import javax.inject.Inject;

import java.io.File;

/**
 * Creates an empty directory.
 */
public class EmptyDirTask extends DefaultTask {

    private File dir;
    private int dirMode = 0755;
    private final Project project;

    @Inject
    public EmptyDirTask(Project project) {
        this.project = project;
    }

    /**
     * Creates an empty directory with the configured permissions.
     */
    @TaskAction
    public void create() {
        dir.mkdirs();
        getChmod().chmod(dir, dirMode);
    }

    @Inject
    public Chmod getChmod() {
        throw new UnsupportedOperationException();
    }

    @Internal
    public File getDir() {
        return dir;
    }

    @Input
    public String getDirPath() {
        return dir.getPath();
    }

    /**
     * @param dir The directory to create
     */
    public void setDir(File dir) {
        this.dir = dir;
    }

    /**
     * @param dir The path of the directory to create. Takes a String and coerces it to a file.
     */
    public void setDir(String dir) {
        this.dir = project.file(dir);
    }

    @Input
    public int getDirMode() {
        return dirMode;
    }

    /**
     * @param dirMode The permissions to apply to the new directory
     */
    public void setDirMode(int dirMode) {
        this.dirMode = dirMode;
    }

}
