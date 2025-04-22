/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.test;

import groovy.lang.Closure;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.testing.Test;

import javax.inject.Inject;

@CacheableTask
public abstract class TestTask extends Test implements TestSuiteConventionMappings {
    private final Project project;

    @Inject
    public TestTask(Project project) {
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
