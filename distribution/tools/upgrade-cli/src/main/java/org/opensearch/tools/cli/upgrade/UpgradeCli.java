/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.upgrade;

import joptsimple.OptionSet;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.cli.EnvironmentAwareCommand;
import org.opensearch.common.collect.Tuple;
import org.opensearch.env.Environment;

/**
 * This class extends the existing opensearch-cli and provides the entry
 * point for the opensearch-upgrade tool.
 * <p>
 * This class is agnostic of the actual logic which performs the upgrade
 * on the node.
 */
public class UpgradeCli extends EnvironmentAwareCommand {

    /**
     * Constructor to create an instance of UpgradeCli.
     */
    public UpgradeCli() {
        super("A CLI tool for upgrading to OpenSearch 1.x from a supported Elasticsearch version.");
    }

    /**
     * Entrypoint for the upgrade tool.
     *
     * @param args args to main.
     * @throws Exception exception thrown during the execution of the UpgradeCli.
     */
    public static void main(String[] args) throws Exception {
        exit(new UpgradeCli().main(args, Terminal.DEFAULT));
    }

    /**
     * Executes the upgrade task. This retrieves an instance of {@link UpgradeTask} which is composed
     * of smaller individual tasks that perform specific operations as part of the overall process.
     *
     * @param terminal current terminal the command is running
     * @param options  options supplied to the command
     * @param env      current environment in which this cli tool is running.
     * @throws UserException if any exception is thrown from the tasks
     */
    @Override
    protected void execute(final Terminal terminal, final OptionSet options, final Environment env) throws UserException {
        try {
            final Tuple<TaskInput, Terminal> input = new Tuple<>(new TaskInput(env), terminal);
            UpgradeTask.getTask().accept(input);
            terminal.println("Done!");
            terminal.println("Next Steps: ");
            terminal.println(" Stop the running elasticsearch on this node.");
            terminal.println(" Start OpenSearch on this node.");
        } catch (RuntimeException ex) {
            throw new UserException(ExitCodes.DATA_ERROR, ex.getMessage());
        }
    }
}
