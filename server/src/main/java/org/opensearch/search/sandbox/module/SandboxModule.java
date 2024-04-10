/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox.module;

import org.opensearch.common.inject.AbstractModule;
import org.opensearch.search.sandbox.SandboxPruner;
import org.opensearch.search.sandbox.tracker.SandboxResourceTracker;
import org.opensearch.search.sandbox.cancellation.SandboxRequestCanceller;
import org.opensearch.search.sandbox.tracker.SandboxResourceTrackerService;

/**
 * Module class for sandboxing related artifacts
 */
public class SandboxModule extends AbstractModule {

    /**
     * Default constructor
     */
    public SandboxModule() {}

    @Override
    protected void configure() {
        bind(SandboxResourceTracker.class).to(SandboxResourceTrackerService.class).asEagerSingleton();
        bind(SandboxRequestCanceller.class).to(SandboxResourceTrackerService.class).asEagerSingleton();
        bind(SandboxPruner.class).to(SandboxResourceTrackerService.class).asEagerSingleton();
    }
}
