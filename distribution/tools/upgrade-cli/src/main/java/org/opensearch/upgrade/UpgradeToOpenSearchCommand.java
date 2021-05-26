/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.opensearch.cli.EnvironmentAwareCommand;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.env.Environment;

import java.util.List;

public class UpgradeToOpenSearchCommand extends EnvironmentAwareCommand {
    private final OptionSpec<String> arguments;
    public UpgradeToOpenSearchCommand() {
        super("Upgrade to OpenSearch", () -> {
        });
        arguments =parser.nonOptions("elasticsearch installation path");
    }


    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final List<String> argumentValues = arguments.values(options);
        if (argumentValues.size() != 1) {
            throw new UserException(ExitCodes.USAGE, "Missing elasticsearch installation location");
        }
        terminal.println("Thanks! we're bringing this out soon!!");
    }

}
