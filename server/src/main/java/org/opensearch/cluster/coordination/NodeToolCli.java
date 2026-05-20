/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.cli.MultiCommand;
import org.opensearch.cli.Terminal;
import org.opensearch.common.cli.CommandLoggingConfigurator;
import org.opensearch.env.NodeRepurposeCommand;
import org.opensearch.env.OverrideNodeVersionCommand;

// NodeToolCli does not extend LoggingAwareCommand, because LoggingAwareCommand performs logging initialization
// after LoggingAwareCommand instance is constructed.
// It's too late for us, because before UnsafeBootstrapClusterManagerCommand is added to the list of subcommands
// log4j2 initialization will happen, because it has static reference to Logger class.
// Even if we avoid making a static reference to Logger class, there is no nice way to avoid declaring
// UNSAFE_BOOTSTRAP, which depends on ClusterService, which in turn has static Logger.
// TODO execute CommandLoggingConfigurator.configureLoggingWithoutConfig() in the constructor of commands, not in beforeMain

/**
 * Command Line Interface tool for Nodes
 *
 * @opensearch.internal
 */
public class NodeToolCli extends MultiCommand {

    public NodeToolCli() {
        super("A CLI tool to do unsafe cluster and index manipulations on current node", () -> {});
        CommandLoggingConfigurator.configureLoggingWithoutConfig();
        subcommands.put("repurpose", new NodeRepurposeCommand());
        subcommands.put("unsafe-bootstrap", new UnsafeBootstrapClusterManagerCommand());
        subcommands.put("detach-cluster", new DetachClusterCommand());
        subcommands.put("override-version", new OverrideNodeVersionCommand());
        subcommands.put("remove-settings", new RemoveSettingsCommand());
        subcommands.put("remove-customs", new RemoveCustomsCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new NodeToolCli().main(args, Terminal.DEFAULT));
    }

}
