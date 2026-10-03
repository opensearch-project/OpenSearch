/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.List;

import picocli.CommandLine.Parameters;

/**
 * Subcommand that resets profiling state and optionally sets a new sample interval.
 */
class ResetCommand extends HeapProfCommand {
    @Parameters(arity = "0..*", paramLabel = "lg_prof_sample", description = "Log2 of bytes between samples")
    private List<String> arguments = new ArrayList<>();

    ResetCommand() {
        super("Reset profiling state (discards data) and set sample interval");
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal) throws Exception {
        int lgSample = 17; // default
        if (!arguments.isEmpty()) {
            try {
                lgSample = Integer.parseInt(arguments.get(0));
            } catch (NumberFormatException e) {
                throw new UserException(1, "lg_prof_sample must be an integer, got: " + arguments.get(0));
            }
        }
        mbs.invoke(mbean, "reset", new Object[] { lgSample }, new String[] { "int" });
        terminal.println("Profiling reset with lg_prof_sample=" + lgSample + " (sample every ~" + ((1L << lgSample) / 1024) + "KB)");
    }
}
