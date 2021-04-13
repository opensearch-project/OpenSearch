/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gradle.precommit;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.TaskProvider;

public class GitSecretsPrecommitPlugin extends PrecommitPlugin {
    @Override
    public TaskProvider<? extends Task> createTask(Project project) {
        return project.getTasks().register("gitSecrets", GitSecretsTask.class);
    }
}
