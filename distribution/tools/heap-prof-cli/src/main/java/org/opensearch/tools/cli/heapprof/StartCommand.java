/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;
import org.opensearch.cli.Terminal;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

class StartCommand extends HeapProfCommand {
    StartCommand() { super("Activate jemalloc heap profiling"); }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        mbs.invoke(mbean, "activate", null, null);
        terminal.println("Heap profiling activated. Allocations are now being sampled.");
    }
}
