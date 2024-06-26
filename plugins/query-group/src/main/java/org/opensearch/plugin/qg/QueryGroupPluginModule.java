/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.qg;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.plugin.qg.service.Persistable;
import org.opensearch.plugin.qg.service.QueryGroupPersistenceService;

/**
 * Guice Module to manage QueryGroup related objects
 */
public class QueryGroupPluginModule extends AbstractModule {

    /**
     * Constructor for QueryGroupPluginModule
     */
    public QueryGroupPluginModule() {}

    @Override
    protected void configure() {
        bind(new TypeLiteral<Persistable<QueryGroup>>() {
        }).to(QueryGroupPersistenceService.class).asEagerSingleton();
    }
}
