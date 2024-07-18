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

package org.opensearch.systemd;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.hamcrest.OptionalMatchers;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SystemdModulePluginTests extends OpenSearchTestCase {
    final Scheduler.Cancellable extender = mock(Scheduler.Cancellable.class);
    final ThreadPool threadPool = mock(ThreadPool.class);

    {
        when(extender.cancel()).thenReturn(true);
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), eq(TimeValue.timeValueSeconds(15)), eq(ThreadPool.Names.SAME)))
            .thenReturn(extender);
    }

    public void testIsImplicitlyNotEnabled() {
        final SystemdModulePlugin plugin = new SystemdModulePlugin(null);
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null, null);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testIsExplicitlyNotEnabled() {
        final SystemdModulePlugin plugin = new SystemdModulePlugin(Boolean.FALSE.toString());
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null, null);
        assertFalse(plugin.isEnabled());
        assertNull(plugin.extender());
    }

    public void testInvalid() {
        final String esSDNotify = randomValueOtherThanMany(
            s -> Boolean.TRUE.toString().equals(s) || Boolean.FALSE.toString().equals(s),
            () -> randomAlphaOfLength(4)
        );
        final RuntimeException e = expectThrows(RuntimeException.class, () -> new SystemdModulePlugin(esSDNotify));
        assertThat(e, hasToString(containsString("OPENSEARCH_SD_NOTIFY set to unexpected value [" + esSDNotify + "]")));
    }

    public void testOnNodeStartedSuccess() {
        runTestOnNodeStarted(Boolean.TRUE.toString(), randomIntBetween(0, Integer.MAX_VALUE), (maybe, plugin) -> {
            assertThat(maybe, OptionalMatchers.isEmpty());
            verify(plugin.extender()).cancel();
        });
    }

    public void testOnNodeStartedFailure() {
        final int rc = randomIntBetween(Integer.MIN_VALUE, -1);
        runTestOnNodeStarted(Boolean.TRUE.toString(), rc, (maybe, plugin) -> {
            assertThat(maybe, OptionalMatchers.isPresent());
            // noinspection OptionalGetWithoutIsPresent
            assertThat(maybe.get(), instanceOf(RuntimeException.class));
            assertThat(maybe.get(), hasToString(containsString("sd_notify returned error [" + rc + "]")));
        });
    }

    public void testOnNodeStartedNotEnabled() {
        runTestOnNodeStarted(Boolean.FALSE.toString(), randomInt(), (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestOnNodeStarted(
        final String esSDNotify,
        final int rc,
        final BiConsumer<Optional<Exception>, SystemdModulePlugin> assertions
    ) {
        runTest(esSDNotify, rc, assertions, SystemdModulePlugin::onNodeStarted, "READY=1");
    }

    public void testCloseSuccess() {
        runTestClose(
            Boolean.TRUE.toString(),
            randomIntBetween(1, Integer.MAX_VALUE),
            (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty())
        );
    }

    public void testCloseFailure() {
        runTestClose(
            Boolean.TRUE.toString(),
            randomIntBetween(Integer.MIN_VALUE, -1),
            (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty())
        );
    }

    public void testCloseNotEnabled() {
        runTestClose(Boolean.FALSE.toString(), randomInt(), (maybe, plugin) -> assertThat(maybe, OptionalMatchers.isEmpty()));
    }

    private void runTestClose(
        final String esSDNotify,
        final int rc,
        final BiConsumer<Optional<Exception>, SystemdModulePlugin> assertions
    ) {
        runTest(esSDNotify, rc, assertions, SystemdModulePlugin::close, "STOPPING=1");
    }

    private void runTest(
        final String esSDNotify,
        final int rc,
        final BiConsumer<Optional<Exception>, SystemdModulePlugin> assertions,
        final CheckedConsumer<SystemdModulePlugin, IOException> invocation,
        final String expectedState
    ) {
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicInteger invokedUnsetEnvironment = new AtomicInteger();
        final AtomicReference<String> invokedState = new AtomicReference<>();
        final SystemdModulePlugin plugin = new SystemdModulePlugin(esSDNotify) {

            @Override
            int sd_notify(final int unset_environment, final String state) {
                invoked.set(true);
                invokedUnsetEnvironment.set(unset_environment);
                invokedState.set(state);
                return rc;
            }

        };
        plugin.createComponents(null, null, threadPool, null, null, null, null, null, null, null, null, null);
        if (Boolean.TRUE.toString().equals(esSDNotify)) {
            assertNotNull(plugin.extender());
        } else {
            assertNull(plugin.extender());
        }

        boolean success = false;
        try {
            invocation.accept(plugin);
            success = true;
        } catch (final Exception e) {
            assertions.accept(Optional.of(e), plugin);
        }
        if (success) {
            assertions.accept(Optional.empty(), plugin);
        }
        if (Boolean.TRUE.toString().equals(esSDNotify)) {
            assertTrue(invoked.get());
            assertThat(invokedUnsetEnvironment.get(), equalTo(0));
            assertThat(invokedState.get(), equalTo(expectedState));
        } else {
            assertFalse(invoked.get());
        }
    }

}
