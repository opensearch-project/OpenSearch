/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.junit.rules.TemporaryFolder;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SecurityPluginCompatibilityIT extends HttpSmokeTestCaseWithIdentity {
    public static class RestHandlerWrapperPlugin extends Plugin implements ActionPlugin {
        public RestHandlerWrapperPlugin() {}

        @Override
        public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
            return UnaryOperator.identity();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = super.nodePlugins().stream().collect(Collectors.toList());
        plugins.add(RestHandlerWrapperPlugin.class);
        return plugins;
    }

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testNodeStartsUpWithTwoRestHandlerWrappers() throws Exception {
        try {
            TemporaryFolder folder = new TemporaryFolder();
            folder.create();
            File internalUsersYml = folder.newFile("internal_users.yml");
            FileWriter fw1 = new FileWriter(internalUsersYml);
            BufferedWriter bw1 = new BufferedWriter(fw1);
            // hash is a bcrypt hash of 'admin'
            bw1.write("admin:\n  hash: \"$2a$12$VcCDgh2NDk07JGN0rjGbM.Ad41qVR/YFJcgHp0UGns5JDymv..TOG\"\n");
            bw1.close();
            final String defaultInitDirectory = folder.getRoot().getAbsolutePath();
            System.setProperty("identity.default_init.dir", defaultInitDirectory);

            startNodes();

            ensureIdentityIndexIsGreen();
        } catch (IOException ioe) {
            fail("error creating temporary test file in " + this.getClass().getSimpleName());
        }
    }
}
