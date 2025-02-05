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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.tools.cli.plugin;

import org.opensearch.cli.Command;
import org.opensearch.cli.Terminal;
import org.opensearch.common.cli.LoggingAwareMultiCommand;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * A CLI tool for adding, removing and listing plugins for OpenSearch.
 */
public class PluginCli extends LoggingAwareMultiCommand {

    private final Collection<Command> commands;

    private PluginCli() {
        super("A tool for managing installed opensearch plugins");
        subcommands.put("list", new ListPluginsCommand());
        subcommands.put("install", new InstallPluginCommand());
        subcommands.put("remove", new RemovePluginCommand());
        commands = Collections.unmodifiableCollection(subcommands.values());
    }

    /**
     * Main entry point for the OpenSearch Plugin CLI tool.
     *
     * @param args  CLI commands for managing plugins.
     * @throws Exception if an exception was encountered executing the command.
     */
    public static void main(String[] args) throws Exception {
        exit(new PluginCli().main(args, Terminal.DEFAULT));
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(commands);
    }

}
