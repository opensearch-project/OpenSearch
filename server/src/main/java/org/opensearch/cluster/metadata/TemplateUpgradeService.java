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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.gateway.GatewayService;
import org.opensearch.indices.IndexTemplateMissingException;
import org.opensearch.plugins.Plugin;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static java.util.Collections.singletonMap;

/**
 * Upgrades Templates on behalf of installed {@link Plugin}s when a node joins the cluster
 *
 * @opensearch.internal
 */
public class TemplateUpgradeService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(TemplateUpgradeService.class);

    private final UnaryOperator<Map<String, IndexTemplateMetadata>> indexTemplateMetadataUpgraders;

    public final ClusterService clusterService;

    public final ThreadPool threadPool;

    public final Client client;

    final AtomicInteger upgradesInProgress = new AtomicInteger();

    private Map<String, IndexTemplateMetadata> lastTemplateMetadata;

    public TemplateUpgradeService(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.indexTemplateMetadataUpgraders = templates -> {
            Map<String, IndexTemplateMetadata> upgradedTemplates = new HashMap<>(templates);
            for (UnaryOperator<Map<String, IndexTemplateMetadata>> upgrader : indexTemplateMetadataUpgraders) {
                upgradedTemplates = upgrader.apply(upgradedTemplates);
            }
            return upgradedTemplates;
        };
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return;
        }
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have the index templates,
            // while they actually do exist
            return;
        }

        if (upgradesInProgress.get() > 0) {
            // we are already running some upgrades - skip this cluster state update
            return;
        }

        final Map<String, IndexTemplateMetadata> templates = state.getMetadata().getTemplates();

        if (templates == lastTemplateMetadata) {
            // we already checked these sets of templates - no reason to check it again
            // we can do identity check here because due to cluster state diffs the actual map will not change
            // if there were no changes
            return;
        }

        lastTemplateMetadata = templates;
        Optional<Tuple<Map<String, BytesReference>, Set<String>>> changes = calculateTemplateChanges(templates);
        if (changes.isPresent()) {
            if (upgradesInProgress.compareAndSet(0, changes.get().v1().size() + changes.get().v2().size() + 1)) {
                logger.info(
                    "Starting template upgrade to version {}, {} templates will be updated and {} will be removed",
                    Version.CURRENT,
                    changes.get().v1().size(),
                    changes.get().v2().size()
                );

                assert threadPool.getThreadContext().isSystemContext();
                threadPool.generic().execute(() -> upgradeTemplates(changes.get().v1(), changes.get().v2()));
            }
        }
    }

    void upgradeTemplates(Map<String, BytesReference> changes, Set<String> deletions) {
        final AtomicBoolean anyUpgradeFailed = new AtomicBoolean(false);
        if (threadPool.getThreadContext().isSystemContext() == false) {
            throw new IllegalStateException("template updates from the template upgrade service should always happen in a system context");
        }

        for (Map.Entry<String, BytesReference> change : changes.entrySet()) {
            PutIndexTemplateRequest request = new PutIndexTemplateRequest(change.getKey()).source(
                change.getValue(),
                MediaTypeRegistry.JSON
            );
            request.clusterManagerNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().putTemplate(request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        logger.warn("Error updating template [{}], request was not acknowledged", change.getKey());
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }

                @Override
                public void onFailure(Exception e) {
                    anyUpgradeFailed.set(true);
                    logger.warn(new ParameterizedMessage("Error updating template [{}]", change.getKey()), e);
                    tryFinishUpgrade(anyUpgradeFailed);
                }
            });
        }

        for (String template : deletions) {
            DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest(template);
            request.clusterManagerNodeTimeout(TimeValue.timeValueMinutes(1));
            client.admin().indices().deleteTemplate(request, new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    if (response.isAcknowledged() == false) {
                        anyUpgradeFailed.set(true);
                        logger.warn("Error deleting template [{}], request was not acknowledged", template);
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }

                @Override
                public void onFailure(Exception e) {
                    anyUpgradeFailed.set(true);
                    if (e instanceof IndexTemplateMissingException == false) {
                        // we might attempt to delete the same template from different nodes - so that's ok if template doesn't exist
                        // otherwise we need to warn
                        logger.warn(new ParameterizedMessage("Error deleting template [{}]", template), e);
                    }
                    tryFinishUpgrade(anyUpgradeFailed);
                }
            });
        }
    }

    void tryFinishUpgrade(AtomicBoolean anyUpgradeFailed) {
        assert upgradesInProgress.get() > 0;
        if (upgradesInProgress.decrementAndGet() == 1) {
            try {
                // this is the last upgrade, the templates should now be in the desired state
                if (anyUpgradeFailed.get()) {
                    logger.info("Templates were partially upgraded to version {}", Version.CURRENT);
                } else {
                    logger.info("Templates were upgraded successfully to version {}", Version.CURRENT);
                }
                // Check upgraders are satisfied after the update completed. If they still
                // report that changes are required, this might indicate a bug or that something
                // else tinkering with the templates during the upgrade.
                final Map<String, IndexTemplateMetadata> upgradedTemplates = clusterService.state().getMetadata().getTemplates();
                final boolean changesRequired = calculateTemplateChanges(upgradedTemplates).isPresent();
                if (changesRequired) {
                    logger.warn("Templates are still reported as out of date after the upgrade. The template upgrade will be retried.");
                }
            } finally {
                final int noMoreUpgrades = upgradesInProgress.decrementAndGet();
                assert noMoreUpgrades == 0;
            }
        }
    }

    Optional<Tuple<Map<String, BytesReference>, Set<String>>> calculateTemplateChanges(final Map<String, IndexTemplateMetadata> templates) {
        // collect current templates
        Map<String, IndexTemplateMetadata> existingMap = new HashMap<>();
        for (Map.Entry<String, IndexTemplateMetadata> customCursor : templates.entrySet()) {
            existingMap.put(customCursor.getKey(), customCursor.getValue());
        }
        // upgrade global custom meta data
        Map<String, IndexTemplateMetadata> upgradedMap = indexTemplateMetadataUpgraders.apply(existingMap);
        if (upgradedMap.equals(existingMap) == false) {
            Set<String> deletes = new HashSet<>();
            Map<String, BytesReference> changes = new HashMap<>();
            // remove templates if needed
            existingMap.keySet().forEach(s -> {
                if (upgradedMap.containsKey(s) == false) {
                    deletes.add(s);
                }
            });
            upgradedMap.forEach((key, value) -> {
                if (value.equals(existingMap.get(key)) == false) {
                    changes.put(key, toBytesReference(value));
                }
            });
            return Optional.of(new Tuple<>(changes, deletes));
        }
        return Optional.empty();
    }

    private static final ToXContent.Params PARAMS = new ToXContent.MapParams(singletonMap("reduce_mappings", "true"));

    private BytesReference toBytesReference(IndexTemplateMetadata templateMetadata) {
        try {
            return XContentHelper.toXContent((builder, params) -> {
                IndexTemplateMetadata.Builder.toInnerXContentWithTypes(templateMetadata, builder, params);
                return builder;
            }, MediaTypeRegistry.JSON, PARAMS, false);
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot serialize template [" + templateMetadata.getName() + "]", ex);
        }
    }
}
