/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.jmx;

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.opensearch.cli.Command;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;

import java.util.List;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Base class for CLI commands that connect to a running OpenSearch process via JMX Attach API.
 * <p>
 * Handles:
 * 1. PID discovery (auto-detect or --pid flag)
 * 2. JVM attach
 * 3. JMX agent loading
 * 4. MBeanServer connection
 * <p>
 * Subclasses implement {@link #execute(MBeanServerConnection, Terminal, OptionSet)} to
 * perform their specific MBean operations.
 * <p>
 * This class can be reused by any CLI tool that needs to interact with a running
 * OpenSearch node via JMX (e.g., heap profiling, native stats, allocator tuning).
 */
public abstract class JmxCommand extends Command {

    private final OptionSpec<String> pidOpt;

    protected JmxCommand(String description) {
        super(description);
        pidOpt = parser.accepts("pid", "OpenSearch process ID (auto-detected if not specified)")
            .withOptionalArg()
            .ofType(String.class);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        String pid = pidOpt.value(options);
        if (pid == null || pid.isEmpty()) {
            pid = findOpenSearchPid();
        }
        if (pid == null) {
            throw new UserException(1, "Could not find running OpenSearch process. Use --pid to specify.");
        }

        terminal.println("Connecting to OpenSearch process (PID: " + pid + ")...");

        VirtualMachine vm = VirtualMachine.attach(pid);
        try {
            String connectorAddr = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
            if (connectorAddr == null) {
                String agent = vm.getSystemProperties().getProperty("java.home")
                    + "/lib/management-agent.jar";
                vm.loadAgent(agent);
                connectorAddr = vm.getAgentProperties().getProperty("com.sun.management.jmxremote.localConnectorAddress");
            }

            JMXServiceURL url = new JMXServiceURL(connectorAddr);
            try (JMXConnector connector = JMXConnectorFactory.connect(url)) {
                MBeanServerConnection mbs = connector.getMBeanServerConnection();
                execute(mbs, terminal, options);
            }
        } finally {
            vm.detach();
        }
    }

    /**
     * Subclasses implement this to perform MBean operations on the connected node.
     */
    protected abstract void execute(MBeanServerConnection mbs, Terminal terminal, OptionSet options) throws Exception;

    private String findOpenSearchPid() {
        List<VirtualMachineDescriptor> vms = VirtualMachine.list();
        for (VirtualMachineDescriptor vmd : vms) {
            String display = vmd.displayName();
            if (display.contains("org.opensearch.bootstrap.OpenSearch")
                || display.contains("org.opensearch.bootstrap.Bootstrap")) {
                return vmd.id();
            }
        }
        return null;
    }
}
