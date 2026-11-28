/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import org.opensearch.cli.Terminal;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * Subcommand that displays the current heap profiling status.
 */
class StatusCommand extends HeapProfCommand {
    StatusCommand() {
        super("Show current heap profiling status");
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal) throws Exception {
        Boolean active = (Boolean) mbs.getAttribute(mbean, "Active");
        terminal.println("Heap profiling active: " + active);
    }
}
