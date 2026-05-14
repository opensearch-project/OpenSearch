/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;
import org.opensearch.cli.Terminal;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

class StatusCommand extends HeapProfCommand {
    StatusCommand() { super("Show current heap profiling status"); }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        Boolean active = (Boolean) mbs.getAttribute(mbean, "Active");
        terminal.println("Heap profiling active: " + active);
    }
}
