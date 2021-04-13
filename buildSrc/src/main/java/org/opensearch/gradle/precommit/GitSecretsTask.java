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

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;

/**
 * Checks for secrets accidentally checked into source files.
 */
public class GitSecretsTask extends DefaultTask {

    public GitSecretsTask() {
        setDescription("Checks source files for secrets that should not be checked in.");
    }

    @TaskAction
    public void runGitSecrets() throws IOException, InterruptedException {
        Process p = Runtime.getRuntime().exec("git secrets --scan");
        p.waitFor();
        if (p.exitValue() != 0) {
            String error = "Failed to run git-secrets, exit code=" + p.exitValue();
            error += "\r\nMake sure that 'git secrets --scan' runs clean.";
            error += "\r\nDid you forget to install it? See https://github.com/awslabs/git-secrets";
            throw new GradleException(error);
        }
    }
}
