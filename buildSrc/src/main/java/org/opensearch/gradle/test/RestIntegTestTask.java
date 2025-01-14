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

package org.opensearch.gradle.test;

import groovy.lang.Closure;

import org.opensearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.CacheableTask;

import javax.inject.Inject;

/**
 * Sub typed version of {@link StandaloneRestIntegTestTask}  that is used to differentiate between plain standalone
 * integ test tasks based on {@link StandaloneRestIntegTestTask} and
 * conventional configured tasks of {@link RestIntegTestTask}
 */
@CacheableTask
public abstract class RestIntegTestTask extends StandaloneRestIntegTestTask implements TestSuiteConventionMappings {
    private final Project project;

    @Inject
    public RestIntegTestTask(Project project) {
        super(project);
        this.project = project;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Task configure(Closure closure) {
        final Task t = super.configure(closure);
        applyConventionMapping(project, getConventionMapping());
        return t;
    }
}
