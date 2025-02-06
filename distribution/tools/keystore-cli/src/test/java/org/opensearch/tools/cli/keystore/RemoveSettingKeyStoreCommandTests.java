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

package org.opensearch.tools.cli.keystore;

import org.opensearch.cli.Command;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.UserException;
import org.opensearch.env.Environment;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;

public class RemoveSettingKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new RemoveSettingKeyStoreCommand() {
            @Override
            protected Environment createEnv(Map<String, String> settings) throws UserException {
                return env;
            }
        };
    }

    public void testMissing() {
        String password = "keystorepassword";
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.DATA_ERROR, e.exitCode);
        assertThat(e.getMessage(), containsString("keystore not found"));
    }

    public void testNoSettings() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, this::execute);
        assertEquals(ExitCodes.USAGE, e.exitCode);
        assertThat(e.getMessage(), containsString("Must supply at least one setting"));
    }

    public void testNonExistentSetting() throws Exception {
        String password = "keystorepassword";
        createKeystore(password);
        terminal.addSecretInput(password);
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(ExitCodes.CONFIG, e.exitCode);
        assertThat(e.getMessage(), containsString("[foo] does not exist"));
    }

    public void testOne() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput(password);
        execute("foo");
        assertFalse(loadKeystore(password).getSettingNames().contains("foo"));
    }

    public void testMany() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "1", "bar", "2", "baz", "3");
        terminal.addSecretInput(password);
        execute("foo", "baz");
        Set<String> settings = loadKeystore(password).getSettingNames();
        assertFalse(settings.contains("foo"));
        assertFalse(settings.contains("baz"));
        assertTrue(settings.contains("bar"));
        assertEquals(2, settings.size()); // account for keystore.seed too
    }

    public void testRemoveWithIncorrectPassword() throws Exception {
        String password = "keystorepassword";
        createKeystore(password, "foo", "bar");
        terminal.addSecretInput("thewrongpassword");
        UserException e = expectThrows(UserException.class, () -> execute("foo"));
        assertEquals(e.getMessage(), ExitCodes.DATA_ERROR, e.exitCode);
        if (inFipsJvm()) {
            assertThat(
                e.getMessage(),
                anyOf(
                    containsString("Provided keystore password was incorrect"),
                    containsString("Keystore has been corrupted or tampered with")
                )
            );
        } else {
            assertThat(e.getMessage(), containsString("Provided keystore password was incorrect"));
        }

    }

    public void testRemoveFromUnprotectedKeystore() throws Exception {
        String password = "";
        createKeystore(password, "foo", "bar");
        // will not be prompted for a password
        execute("foo");
        assertFalse(loadKeystore(password).getSettingNames().contains("foo"));
    }
}
