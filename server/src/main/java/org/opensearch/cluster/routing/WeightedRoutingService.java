/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING;

/**
 * * Service responsible for updating cluster state metadata with weighted routing weights
 */
public class WeightedRoutingService {
    private static final Logger logger = LogManager.getLogger(WeightedRoutingService.class);
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private volatile List<String> awarenessAttributes;
    private volatile Map<String, List<String>> forcedAwarenessAttributes;
    private static final Double DECOMMISSIONED_AWARENESS_VALUE_WEIGHT = 0.0;

    @Inject
    public WeightedRoutingService(
        ClusterService clusterService,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.awarenessAttributes = AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING,
            this::setAwarenessAttributes
        );
        setForcedAwarenessAttributes(CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.get(settings));
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING,
            this::setForcedAwarenessAttributes
        );

    }

    public void registerWeightedRoutingMetadata(
        final ClusterPutWeightedRoutingRequest request,
        final ActionListener<ClusterStateUpdateResponse> listener
    ) {
        final WeightedRouting newWeightedRouting = new WeightedRouting(request.getWeightedRouting());

        final long requestVersion = request.getVersion();
        clusterService.submitStateUpdateTask("update_weighted_routing", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                // verify that request object has weights for all discovered and forced awareness values
                ensureWeightsSetForAllDiscoveredAndForcedAwarenessValues(currentState, request);
                // verify weights will not be updated for a decommissioned attribute
                ensureDecommissionedAttributeHasZeroWeight(currentState, request);
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                WeightedRoutingMetadata weightedRoutingMetadata = metadata.custom(WeightedRoutingMetadata.TYPE);
                ensureNoVersionConflict(requestVersion, weightedRoutingMetadata);
                if (weightedRoutingMetadata == null) {
                    logger.info("add weighted routing weights in metadata [{}]", newWeightedRouting);
                    weightedRoutingMetadata = new WeightedRoutingMetadata(newWeightedRouting, requestVersion + 1);
                } else {
                    if (!newWeightedRouting.equals(weightedRoutingMetadata.getWeightedRouting())) {
                        logger.info("updated weighted routing weights [{}] in metadata", newWeightedRouting);
                        weightedRoutingMetadata = new WeightedRoutingMetadata(newWeightedRouting, requestVersion + 1);
                    } else {
                        logger.info("weights are same, not updating weighted routing weights [{}] in metadata", newWeightedRouting);
                        return currentState;
                    }
                }
                mdBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
                logger.info("building cluster state with weighted routing weights [{}]", newWeightedRouting);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn(() -> new ParameterizedMessage("failed to update cluster state for weighted routing weights [{}]", e));
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.debug("cluster weighted routing weights metadata change is processed by all the nodes");
                listener.onResponse(new ClusterStateUpdateResponse(true));
            }
        });
    }

    public void deleteWeightedRoutingMetadata(
        final ClusterDeleteWeightedRoutingRequest request,
        final ActionListener<ClusterDeleteWeightedRoutingResponse> listener
    ) {
        final long requestVersion = request.getVersion();
        final String awarenessAttribute = request.getAwarenessAttribute();
        clusterService.submitStateUpdateTask("delete_weighted_routing", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                logger.info("Deleting weighted routing metadata from the cluster state");
                Metadata metadata = currentState.metadata();
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
                WeightedRoutingMetadata weightedRoutingMetadata = metadata.custom(WeightedRoutingMetadata.TYPE);
                ensureNoVersionConflict(requestVersion, weightedRoutingMetadata);

                if ((weightedRoutingMetadata != null && awarenessAttribute == null)
                    || (weightedRoutingMetadata != null
                        && weightedRoutingMetadata.getWeightedRouting().attributeName().equals(awarenessAttribute))) {
                    weightedRoutingMetadata = new WeightedRoutingMetadata(new WeightedRouting(), weightedRoutingMetadata.getVersion() + 1);
                } else {
                    throw new ResourceNotFoundException(
                        String.format(
                            Locale.ROOT,
                            "weighted routing metadata does not have weights set for awareness attribute %s",
                            awarenessAttribute
                        )
                    );
                }

                mdBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
                logger.info("building cluster state with weighted routing weights deleted");
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("failed to remove weighted routing metadata from cluster state", e);
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.debug("cluster weighted routing metadata change is processed by all the nodes");
                listener.onResponse(new ClusterDeleteWeightedRoutingResponse(true));
            }
        });
    }

    List<String> getAwarenessAttributes() {
        return awarenessAttributes;
    }

    private void setAwarenessAttributes(List<String> awarenessAttributes) {
        this.awarenessAttributes = awarenessAttributes;
    }

    private void setForcedAwarenessAttributes(Settings forceSettings) {
        Map<String, List<String>> forcedAwarenessAttributes = new HashMap<>();
        Map<String, Settings> forceGroups = forceSettings.getAsGroups();
        for (Map.Entry<String, Settings> entry : forceGroups.entrySet()) {
            List<String> aValues = entry.getValue().getAsList("values");
            if (aValues.size() > 0) {
                forcedAwarenessAttributes.put(entry.getKey(), aValues);
            }
        }
        this.forcedAwarenessAttributes = forcedAwarenessAttributes;
    }

    public void verifyAwarenessAttribute(String attributeName) {
        if (getAwarenessAttributes().contains(attributeName) == false) {
            ActionRequestValidationException validationException = null;
            validationException = addValidationError(
                String.format(Locale.ROOT, "invalid awareness attribute %s requested for weighted routing", attributeName),
                validationException
            );
            throw validationException;
        }
    }

    private void ensureWeightsSetForAllDiscoveredAndForcedAwarenessValues(ClusterState state, ClusterPutWeightedRoutingRequest request) {
        String attributeName = request.getWeightedRouting().attributeName();
        // build attr_value -> nodes map
        ObjectIntHashMap<String> nodesPerAttribute = state.getRoutingNodes().nodesPerAttributesCounts(attributeName);
        Set<String> discoveredAwarenessValues = new HashSet<>();
        for (ObjectCursor<String> stringObjectCursor : nodesPerAttribute.keys()) {
            if (stringObjectCursor.value != null) discoveredAwarenessValues.add(stringObjectCursor.value);
        }
        Set<String> allAwarenessValues;
        if (forcedAwarenessAttributes.get(attributeName) == null) {
            allAwarenessValues = new HashSet<>();
        } else {
            allAwarenessValues = new HashSet<>(forcedAwarenessAttributes.get(attributeName));
        }
        allAwarenessValues.addAll(discoveredAwarenessValues);
        AtomicInteger countWithZeroWeight = new AtomicInteger();
        allAwarenessValues.forEach(awarenessValue -> {
            if (request.getWeightedRouting().weights().containsKey(awarenessValue) == false) {
                throw new UnsupportedWeightedRoutingStateException(
                    "weight for ["
                        + awarenessValue
                        + "] is not set and it is part of forced awareness value or a routing node has this attribute."
                );
            }
            if (request.getWeightedRouting().weights().get(awarenessValue) == 0) {
                countWithZeroWeight.addAndGet(1);
            }
        });
        // We have validations in place to check that not more than half of the values weights are set to 0 in the request object
        // Adding this check again here on allAwarenessValues such that in no case we land up in a situation where more than half of
        // discovered awareness values has weight zero
        if (countWithZeroWeight.get() > allAwarenessValues.size() / 2) {
            throw addValidationError(
                (String.format(
                    Locale.ROOT,
                    "There are too many discovered attribute values [%s] given zero weight [%d]. Maximum expected number of routing weights having zero weight is [%d]",
                    request.getWeightedRouting().weights().toString(),
                    countWithZeroWeight.get(),
                    allAwarenessValues.size() / 2
                )),
                null
            );
        }
    }

    private void ensureDecommissionedAttributeHasZeroWeight(ClusterState state, ClusterPutWeightedRoutingRequest request) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata == null || decommissionAttributeMetadata.status().equals(DecommissionStatus.FAILED)) {
            // here either there's no decommission action is ongoing or it is in failed state. In this case, we will allow weight update
            return;
        }
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
        WeightedRouting weightedRouting = request.getWeightedRouting();
        if (weightedRouting.attributeName().equals(decommissionAttribute.attributeName()) == false) {
            // this is unexpected when a different attribute is requested for decommission and weight update is on another attribute
            throw new UnsupportedWeightedRoutingStateException(
                "decommission action ongoing for attribute [{}], cannot update weight for [{}]",
                decommissionAttribute.attributeName(),
                weightedRouting.attributeName()
            );
        }
        if (weightedRouting.weights().containsKey(decommissionAttribute.attributeValue()) == false) {
            // weight of an attribute undergoing decommission must be specified
            throw new UnsupportedWeightedRoutingStateException(
                "weight for [{}] is not specified. Please specify its weight to [{}] as it is under decommission action",
                decommissionAttribute.attributeValue(),
                DECOMMISSIONED_AWARENESS_VALUE_WEIGHT
            );
        }
        if (Objects.equals(
            weightedRouting.weights().get(decommissionAttribute.attributeValue()),
            DECOMMISSIONED_AWARENESS_VALUE_WEIGHT
        ) == false) {
            throw new UnsupportedWeightedRoutingStateException(
                "weight for [{}] must be set to [{}] as it is under decommission action",
                decommissionAttribute.attributeValue(),
                DECOMMISSIONED_AWARENESS_VALUE_WEIGHT
            );
        }
    }

    private void ensureNoVersionConflict(long requestedVersion, WeightedRoutingMetadata weightedRoutingMetadata) {
        if ((weightedRoutingMetadata == null && requestedVersion != WeightedRoutingMetadata.INITIAL_VERSION)
            || (weightedRoutingMetadata != null && requestedVersion != weightedRoutingMetadata.getVersion())) {
            throw new UnsupportedWeightedRoutingStateException(
                String.format(
                    Locale.ROOT,
                    "requested version is %s but cluster weighted routing metadata is at a " + "different version %s ",
                    requestedVersion,
                    weightedRoutingMetadata != null ? weightedRoutingMetadata.getVersion() : WeightedRoutingMetadata.INITIAL_VERSION
                )
            );
        }
    }
}
