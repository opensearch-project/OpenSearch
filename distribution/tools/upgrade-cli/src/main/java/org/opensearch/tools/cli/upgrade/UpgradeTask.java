/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import org.opensearch.cli.Terminal;
import org.opensearch.common.collect.Tuple;

import java.util.function.Consumer;

/**
 * An interface for an upgrade task, which in this instance is an unit of
 * operation that is part of the overall upgrade process. This extends the
 * {@link java.util.function.Consumer} interface.
 * <p>
 * The implementing tasks consume and instance of a tuple of {@link TaskInput}
 * and {@link Terminal} and operate via side effects.
 *
 */
interface UpgradeTask extends Consumer<Tuple<TaskInput, Terminal>> {
    /**
     * Composes the individual tasks to create a pipeline for the overall upgrade task.
     *
     * @return an instance of {@link java.util.function.Consumer} that takes a tuple of
     * task input and the current terminal. The composed task fails if any of the
     * individual tasks fails.
     */
    static Consumer<Tuple<TaskInput, Terminal>> getTask() {
        return new DetectEsInstallationTask().andThen(new ValidateInputTask())
            .andThen(new ImportYmlConfigTask())
            .andThen(new ImportJvmOptionsTask())
            .andThen(new ImportLog4jPropertiesTask())
            .andThen(new InstallPluginsTask())
            .andThen(new ImportKeystoreTask());
    }
}
