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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class SystemdModulePlugin extends Plugin implements ClusterPlugin {

    private static final Logger logger = LogManager.getLogger(SystemdModulePlugin.class);

    private final boolean enabled;

    final boolean isEnabled() {
        return enabled;
    }

    @SuppressWarnings("unused")
    public SystemdModulePlugin() {
        this(System.getenv("OPENSEARCH_SD_NOTIFY"));
    }

    SystemdModulePlugin(final String esSDNotify) {
        logger.trace("OPENSEARCH_SD_NOTIFY is set to [{}]", esSDNotify);
        if (esSDNotify == null) {
            enabled = false;
            return;
        }
        if (Boolean.TRUE.toString().equals(esSDNotify) == false && Boolean.FALSE.toString().equals(esSDNotify) == false) {
            throw new RuntimeException("OPENSEARCH_SD_NOTIFY set to unexpected value [" + esSDNotify + "]");
        }
        enabled = Boolean.TRUE.toString().equals(esSDNotify);
    }

    private final SetOnce<Scheduler.Cancellable> extender = new SetOnce<>();

    Scheduler.Cancellable extender() {
        return extender.get();
    }

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver expressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        if (enabled == false) {
            extender.set(null);
            return Collections.emptyList();
        }
        /*
         * Since we have set the service type to notify, by default systemd will wait up to sixty seconds for the process to send the
         * READY=1 status via sd_notify. Since our startup can take longer than that (e.g., if we are upgrading on-disk metadata) then we
         * need to repeatedly notify systemd that we are still starting up by sending EXTEND_TIMEOUT_USEC with an extension to the timeout.
         * Therefore, every fifteen seconds we send systemd a message via sd_notify to extend the timeout by thirty seconds. We will cancel
         * this scheduled task after we successfully notify systemd that we are ready.
         */
        extender.set(threadPool.scheduleWithFixedDelay(() -> {
            final int rc = sd_notify(0, "EXTEND_TIMEOUT_USEC=30000000");
            if (rc < 0) {
                logger.warn("extending startup timeout via sd_notify failed with [{}]", rc);
            }
        }, TimeValue.timeValueSeconds(15), ThreadPool.Names.SAME));
        return Collections.emptyList();
    }

    int sd_notify(@SuppressWarnings("SameParameterValue") final int unset_environment, final String state) {
        final int rc = Libsystemd.sd_notify(unset_environment, state);
        logger.trace("sd_notify({}, {}) returned [{}]", unset_environment, state, rc);
        return rc;
    }

    @Override
    public void onNodeStarted() {
        if (enabled == false) {
            assert extender.get() == null;
            return;
        }
        final int rc = sd_notify(0, "READY=1");
        if (rc < 0) {
            // treat failure to notify systemd of readiness as a startup failure
            throw new RuntimeException("sd_notify returned error [" + rc + "]");
        }
        assert extender.get() != null;
        final boolean cancelled = extender.get().cancel();
        assert cancelled;
    }

    @Override
    public void close() {
        if (enabled == false) {
            return;
        }
        final int rc = sd_notify(0, "STOPPING=1");
        if (rc < 0) {
            // do not treat failure to notify systemd of stopping as a failure
            logger.warn("sd_notify returned error [{}]", rc);
        }
    }

}
