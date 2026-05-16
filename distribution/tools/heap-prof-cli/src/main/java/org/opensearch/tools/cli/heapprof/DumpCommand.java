/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

class DumpCommand extends HeapProfCommand {
    private final OptionSpec<String> pathArg;

    DumpCommand() {
        super("Dump heap profile to a file");
        pathArg = parser.nonOptions("output file path").ofType(String.class);
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        var paths = pathArg.values(options);
        if (paths.isEmpty()) {
            throw new UserException(1, "Usage: opensearch-heap-prof dump <path>");
        }
        String path = paths.get(0);
        String result = (String) mbs.invoke(mbean, "dump", new Object[]{path}, new String[]{"java.lang.String"});
        terminal.println("Heap profile dumped to: " + result);
    }
}
