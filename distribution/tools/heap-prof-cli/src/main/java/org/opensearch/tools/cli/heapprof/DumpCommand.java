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
 * Subcommand that dumps the current heap profile to a specified file path.
 */
class DumpCommand extends HeapProfCommand {
    @Parameters(arity = "0..*", paramLabel = "path", description = "Output file path")
    private List<String> paths = new ArrayList<>();

    DumpCommand() {
        super("Dump heap profile to a file");
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal) throws Exception {
        if (paths.isEmpty()) {
            throw new UserException(1, "Usage: opensearch-heap-prof dump <path>");
        }
        String path = paths.get(0);
        String result = (String) mbs.invoke(mbean, "dump", new Object[] { path }, new String[] { "java.lang.String" });
        terminal.println("Heap profile dumped to: " + result);
    }
}
