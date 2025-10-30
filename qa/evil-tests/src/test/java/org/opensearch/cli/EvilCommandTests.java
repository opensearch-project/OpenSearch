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

package org.opensearch.cli;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;

public class EvilCommandTests extends OpenSearchTestCase {

    public void testCommandShutdownHook() throws Exception {
        final AtomicBoolean closed = new AtomicBoolean();
        final boolean shouldThrow = randomBoolean();

        final Command command = new Command("test-command-shutdown-hook", () -> {}) {
            @Override
            protected void execute(Terminal terminal) throws Exception {
                // no-op; we're only testing the shutdown hook & close()
            }

            @Override
            public void close() throws IOException {
                closed.set(true);
                if (shouldThrow) {
                    throw new IOException("fail");
                }
            }
        };

        final MockTerminal terminal = new MockTerminal();
        command.main(new String[0], terminal);

        assertNotNull(command.getShutdownHookThread());
        // successful removal here asserts that the runtime hook was installed in Command#main
        assertTrue(Runtime.getRuntime().removeShutdownHook(command.getShutdownHookThread()));

        // Manually run & wait for the hook to complete to verify close() + error reporting
        command.getShutdownHookThread().run();
        command.getShutdownHookThread().join();

        assertTrue(closed.get());

        final String output = terminal.getErrorOutput();
        if (shouldThrow) {
            // ensure that we dump the exception
            assertThat(output, containsString("java.io.IOException: fail"));
            // ensure that we dump the stack trace too
            assertThat(output, containsString("\tat org.opensearch.cli.EvilCommandTests$1.close"));
        } else {
            assertThat(output, is(emptyString()));
        }
    }
}
