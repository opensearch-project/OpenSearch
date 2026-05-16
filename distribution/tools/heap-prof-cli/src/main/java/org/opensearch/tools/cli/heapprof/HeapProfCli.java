/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.cli.heapprof;

import org.opensearch.cli.MultiCommand;
import org.opensearch.cli.Terminal;
import org.opensearch.common.cli.CommandLoggingConfigurator;

/**
 * CLI tool for on-demand jemalloc heap profiling of the native layer.
 * <p>
 * Connects to the running OpenSearch JVM on the local node via JMX Attach API
 * and invokes heap profiling operations — no REST API or cluster settings required.
 * <p>
 * Usage:
 *   opensearch-heap-prof start              - Activate heap profiling
 *   opensearch-heap-prof stop               - Deactivate heap profiling
 *   opensearch-heap-prof dump <path>        - Dump heap profile to file
 *   opensearch-heap-prof reset <lg_sample>  - Reset with new sample interval
 *   opensearch-heap-prof status             - Show profiling status
 */
public class HeapProfCli extends MultiCommand {

    public HeapProfCli() {
        super("Native heap profiling tool (jemalloc) — connects to running OpenSearch via JMX", () -> {});
        CommandLoggingConfigurator.configureLoggingWithoutConfig();
        subcommands.put("start", new StartCommand());
        subcommands.put("stop", new StopCommand());
        subcommands.put("dump", new DumpCommand());
        subcommands.put("reset", new ResetCommand());
        subcommands.put("status", new StatusCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new HeapProfCli().main(args, Terminal.DEFAULT));
    }
}
