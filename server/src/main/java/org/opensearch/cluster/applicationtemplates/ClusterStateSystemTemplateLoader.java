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
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.ComponentTemplate;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OriginSettingClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Class responsible for loading the component templates provided by a repository into the cluster state.
 */
@ExperimentalApi
public class ClusterStateSystemTemplateLoader implements SystemTemplateLoader {

    private final Client client;

    private final Supplier<ClusterState> clusterStateSupplier;

    private static final Logger logger = LogManager.getLogger(SystemTemplateLoader.class);

    public static final String TEMPLATE_LOADER_IDENTIFIER = "system_template_loader";
    public static final String TEMPLATE_TYPE_KEY = "_type";

    public ClusterStateSystemTemplateLoader(Client client, Supplier<ClusterState> clusterStateSupplier) {
        this.client = new OriginSettingClient(client, TEMPLATE_LOADER_IDENTIFIER);
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    public boolean loadTemplate(SystemTemplate template) throws IOException {
        final ComponentTemplate existingTemplate = clusterStateSupplier.get()
            .metadata()
            .componentTemplates()
            .get(template.templateMetadata().fullyQualifiedName());

        if (existingTemplate != null
            && !SystemTemplateMetadata.COMPONENT_TEMPLATE_TYPE.equals(
                Objects.toString(existingTemplate.metadata().get(TEMPLATE_TYPE_KEY))
            )) {
            throw new OpenSearchCorruptionException(
                "Attempting to create " + template.templateMetadata().name() + " which has already been created through some other source."
            );
        }

        if (existingTemplate != null && existingTemplate.version() >= template.templateMetadata().version()) {
            logger.debug(
                "Skipping putting template {} as its existing version [{}] is >= fetched version [{}]",
                template.templateMetadata().fullyQualifiedName(),
                existingTemplate.version(),
                template.templateMetadata().version()
            );
            return false;
        }

        ComponentTemplate newTemplate = null;
        try (
            XContentParser contentParser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                template.templateContent().utf8ToString()
            )
        ) {
            newTemplate = ComponentTemplate.parse(contentParser);
        }

        if (!Objects.equals(newTemplate.version(), template.templateMetadata().version())) {
            throw new OpenSearchCorruptionException(
                "Template version mismatch for "
                    + template.templateMetadata().name()
                    + ". Version in metadata: "
                    + template.templateMetadata().version()
                    + " , Version in content: "
                    + newTemplate.version()
            );
        }

        final PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(
            template.templateMetadata().fullyQualifiedName()
        ).componentTemplate(newTemplate);

        return client.admin()
            .indices()
            .execute(PutComponentTemplateAction.INSTANCE, request)
            .actionGet(TimeValue.timeValueMillis(30000))
            .isAcknowledged();
    }
}
