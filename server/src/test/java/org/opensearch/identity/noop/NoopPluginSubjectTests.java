/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.common.settings.Settings;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.Subject;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NoopPluginSubjectTests extends OpenSearchTestCase {
    public static class TestPlugin extends Plugin implements IdentityAwarePlugin {
        private Subject subject;

        @Override
        public void assignSubject(Subject subject) {
            this.subject = subject;
        }

        public Subject getSubject() {
            return subject;
        }
    }

    public void testInitializeIdentityAwarePlugin() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        IdentityService identityService = new IdentityService(Settings.EMPTY, threadPool, List.of());

        TestPlugin testPlugin = new TestPlugin();
        identityService.initializeIdentityAwarePlugins(List.of(testPlugin));

        Subject testPluginSubject = new NoopPluginSubject(threadPool);
        assertThat(testPlugin.getSubject().getPrincipal().getName(), equalTo(NamedPrincipal.UNAUTHENTICATED.getName()));
        assertThat(testPluginSubject.getPrincipal().getName(), equalTo(NamedPrincipal.UNAUTHENTICATED.getName()));
        threadPool.getThreadContext().putHeader("test_header", "foo");
        assertThat(threadPool.getThreadContext().getHeader("test_header"), equalTo("foo"));
        testPluginSubject.runAs(() -> {
            assertNull(threadPool.getThreadContext().getHeader("test_header"));
            return null;
        });
        assertThat(threadPool.getThreadContext().getHeader("test_header"), equalTo("foo"));
        terminate(threadPool);
    }
}
