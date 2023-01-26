/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UserPersistenceIT extends HttpSmokeTestCaseWithIdentity {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = super.nodePlugins().stream().collect(Collectors.toList());
        return plugins;
    }

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testUserPersistence() throws Exception {
        try {
            Path configDir = createTempDir();
            Path internalUsersYaml = configDir.resolve("internal_users.yml");
            Path internalUsersFile = Files.createFile(internalUsersYaml);
            BufferedWriter bw1 = Files.newBufferedWriter(internalUsersFile);
            bw1.write(
                "new-user:\n"
                    + "  hash: \"$2y$12$88IFVl6IfIwCFh5aQYfOmuXVL9j2hz/GusQb35o.4sdTDAEMTOD.K\"\n"
                    + "  attributes:\n"
                    + "    attribute1: \"value1\"\n"
                    + "\n"
                    + "## Demo users\n"
                    + "\n"
                    + "kibanaserver:\n"
                    + "  hash: \"$2a$12$4AcgAt3xwOWadA5s5blL6ev39OXDNhmOesEoo33eZtrq2N0YrU3H.\"\n"
                    + "\n"
                    + "kibanaro:\n"
                    + "  hash: \"$2a$12$JJSXNfTowz7Uu5ttXfeYpeYE0arACvcwlPBStB1F.MI7f0U9Z4DGC\"\n"
                    + "  attributes:\n"
                    + "    attribute1: \"value1\"\n"
                    + "    attribute2: \"value2\"\n"
                    + "    attribute3: \"value3\"\n"
                    + "\n"
                    + "logstash:\n"
                    + "  hash: \"$2a$12$u1ShR4l4uBS3Uv59Pa2y5.1uQuZBrZtmNfqB3iM/.jL0XoV9sghS2\"\n"
                    + "\n"
                    + "readall:\n"
                    + "  hash: \"$2a$12$ae4ycwzwvLtZxwZ82RmiEunBbIPiAmGZduBAjKN0TXdwQFtCwARz2\"\n"
                    + "\n"
                    + "snapshotrestore:\n"
                    + "  hash: \"$2y$12$DpwmetHKwgYnorbgdvORCenv4NAK8cPUg8AI6pxLCuWf/ALc0.v7W\"\n"
                    + "\n"
            );
            bw1.close();
            System.setProperty("identity.default_init.dir", configDir.toString());

            startNodes();

            ensureIdentityIndexIsGreen();
        } catch (IOException ioe) {
            fail("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }

    /**
     * This test verifies that identity module can initialize with invalid yml supplied, for this test a user without a
     * hash is supplied in the internal_users.yml file
     *
     * The node should start up with invalid config.
     *
     * TODO Should this prevent node startup, log with warnings, or what should be intended behavior?
     *
     * @throws Exception - This test should not throw an exception
     */
    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testUserPersistenceInvalidYml() throws Exception {
        try {
            Path configDir = createTempDir();
            Path internalUsersYaml = configDir.resolve("internal_users.yml");
            Path internalUsersFile = Files.createFile(internalUsersYaml);
            BufferedWriter bw1 = Files.newBufferedWriter(internalUsersFile);
            bw1.write(
                "# Invalid internal_users.yml, hash is required\n"
                    + "new-user:\n"
                    + "  attributes:\n"
                    + "    attribute1: \"value1\"\n"
                    + "\n"
            );
            bw1.close();
            System.setProperty("identity.default_init.dir", configDir.toString());

            startNodes();

            ensureIdentityIndexIsGreen();
        } catch (IOException ioe) {
            fail("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }
}
