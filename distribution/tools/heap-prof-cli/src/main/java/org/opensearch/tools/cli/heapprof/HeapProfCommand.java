/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;

import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.tools.cli.jmx.JmxCommand;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * Base class for heap profiling commands. Extends {@link JmxCommand} and adds
 * MBean discovery for the {@code org.opensearch.native:type=HeapProfiler} MBean.
 */
abstract class HeapProfCommand extends JmxCommand {

    private static final String MBEAN_NAME = "org.opensearch.native:type=HeapProfiler";

    HeapProfCommand(String description) {
        super(description);
    }

    @Override
    protected void execute(MBeanServerConnection mbs, Terminal terminal, OptionSet options) throws Exception {
        ObjectName mbean = new ObjectName(MBEAN_NAME);
        if (!mbs.isRegistered(mbean)) {
            throw new UserException(1,
                "HeapProfiler MBean not registered. Ensure the native-bridge module is loaded "
                + "and the native library was built with profiling support.");
        }
        invokeOnMBean(mbs, mbean, terminal, options);
    }

    protected abstract void invokeOnMBean(
        MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options
    ) throws Exception;
}
