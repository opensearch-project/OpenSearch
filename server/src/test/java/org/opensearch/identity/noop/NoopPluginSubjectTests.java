/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.Version;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.identity.IdentityService;
import org.opensearch.identity.NamedPrincipal;
import org.opensearch.identity.PluginSubject;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NoopPluginSubjectTests extends OpenSearchTestCase {
    public static class TestPlugin extends Plugin implements IdentityAwarePlugin {
        private PluginSubject subject;

        @Override
        public void assignSubject(PluginSubject subject) {
            this.subject = subject;
        }

        public PluginSubject getSubject() {
            return subject;
        }
    }

    public void testInitializeIdentityAwarePlugin() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        IdentityService identityService = new IdentityService(Settings.EMPTY, threadPool, List.of());

        TestPlugin testPlugin = new TestPlugin();
        final PluginInfo info = new PluginInfo(
            "fake",
            "foo",
            "x.y.z",
            Version.CURRENT,
            "1.8",
            testPlugin.getClass().getCanonicalName(),
            "folder",
            Collections.emptyList(),
            false,
            Settings.EMPTY
        );
        identityService.initializeIdentityAwarePlugins(List.of(Tuple.tuple(info, testPlugin)));

        PluginSubject testPluginSubject = new NoopPluginSubject(threadPool);
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
