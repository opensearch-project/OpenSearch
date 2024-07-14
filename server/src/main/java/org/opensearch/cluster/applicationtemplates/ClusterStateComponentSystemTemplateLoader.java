/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.applicationtemplates;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.client.Client;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Class reponsible for loading the component templates provided by a repository into the cluster state.
 */
@ExperimentalApi
public class ClusterStateComponentSystemTemplateLoader implements SystemTemplateLoader {

    private final Client client;

    private final Supplier<ClusterState> clusterStateSupplier;

    private static final Logger logger = LogManager.getLogger(SystemTemplateLoader.class);

    public static final String TEMPLATE_LOADER_IDENTIFIER = "system_template_loader";

    public ClusterStateComponentSystemTemplateLoader(Client client,
                                                     ThreadPool threadPool,
                                                     Supplier<ClusterState> clusterStateSupplier) {
        this.client = new OriginSettingClient(client, TEMPLATE_LOADER_IDENTIFIER);
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    public void loadTemplate(SystemTemplate template) throws IOException {
        ComponentTemplate existingTemplate = clusterStateSupplier.get().metadata().componentTemplates().get(template.templateInfo().fullyQualifiedName());

        XContentParser contentParser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS,
            template.templateContent().utf8ToString());
        ComponentTemplate newTemplate = ComponentTemplate.parse(contentParser);

        if (existingTemplate != null && existingTemplate.version() >= newTemplate.version()) {
            logger.debug("Skipping putting template {} as its existing version [{}] is >= fetched version [{}]", template.templateInfo().fullyQualifiedName(),
                existingTemplate.version(),
                newTemplate.version());
        }

        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(template.templateInfo().fullyQualifiedName())
            .componentTemplate(newTemplate);

        client.admin().indices().execute(PutComponentTemplateAction.INSTANCE, request).actionGet(TimeValue.timeValueMillis(30000));
    }
}
