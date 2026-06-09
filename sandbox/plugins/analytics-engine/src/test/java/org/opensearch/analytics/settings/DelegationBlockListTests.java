/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DelegationBlockListTests extends OpenSearchTestCase {

    /** Lucene-like backend: accepts FILTER delegation and ships serializers for EQUALS + LIKE. */
    private static final String LUCENE = "lucene";

    private CapabilityRegistry registry() {
        AnalyticsSearchBackendPlugin lucene = new AnalyticsSearchBackendPlugin() {
            @Override
            public String name() {
                return LUCENE;
            }

            @Override
            public BackendCapabilityProvider getCapabilityProvider() {
                return new BackendCapabilityProvider() {
                    @Override
                    public Set<EngineCapability> supportedEngineCapabilities() {
                        return Set.of();
                    }

                    @Override
                    public Set<DelegationType> acceptedDelegations() {
                        return Set.of(DelegationType.FILTER);
                    }
                };
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                // Only EQUALS and LIKE are delegatable (have serializers) in this fixture.
                DelegatedPredicateSerializer stub = (call, fieldStorage) -> new byte[0];
                return Map.of(ScalarFunction.EQUALS, stub, ScalarFunction.LIKE, stub);
            }
        };
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory = FieldStorageResolver::new;
        return new CapabilityRegistry(List.of(lucene), fieldStorageFactory);
    }

    private ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(settings, Set.of(AnalyticsQuerySettings.DELEGATION_BLOCKED_PREDICATES));
    }

    public void testEmptyBlocksNothing() {
        DelegationBlockList blockList = DelegationBlockList.empty();
        assertTrue(blockList.isEmpty());
        assertFalse(blockList.isBlocked(LUCENE, ScalarFunction.LIKE));
    }

    public void testSeedFromClusterSettings() {
        Settings settings = Settings.builder().putList("analytics.delegation.lucene.blocked_predicates", "LIKE", "equals").build();
        DelegationBlockList blockList = DelegationBlockList.create(clusterSettings(settings), settings, registry());
        assertTrue(blockList.isBlocked(LUCENE, ScalarFunction.LIKE));
        assertTrue("case-insensitive token parsed", blockList.isBlocked(LUCENE, ScalarFunction.EQUALS));
    }

    public void testDynamicUpdate() {
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        DelegationBlockList blockList = DelegationBlockList.create(clusterSettings, Settings.EMPTY, registry());
        assertTrue(blockList.isEmpty());

        clusterSettings.applySettings(Settings.builder().putList("analytics.delegation.lucene.blocked_predicates", "LIKE").build());
        assertTrue(blockList.isBlocked(LUCENE, ScalarFunction.LIKE));

        // Clearing the list removes the backend's block entry.
        clusterSettings.applySettings(Settings.builder().putList("analytics.delegation.lucene.blocked_predicates").build());
        assertFalse(blockList.isBlocked(LUCENE, ScalarFunction.LIKE));
        assertTrue(blockList.isEmpty());
    }

    public void testUnknownPredicateNameRejectedByElementParser() {
        // A token that isn't a ScalarFunction fails the list element parser at update time.
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        DelegationBlockList.create(clusterSettings, Settings.EMPTY, registry());
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().putList("analytics.delegation.lucene.blocked_predicates", "NONSENSE").build()
            )
        );
    }

    public void testNonAcceptorBackendNamespaceRejected() {
        // datafusion is not a FILTER-delegation acceptor → the namespace is rejected at update time.
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        DelegationBlockList.create(clusterSettings, Settings.EMPTY, registry());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().putList("analytics.delegation.datafusion.blocked_predicates", "LIKE").build()
            )
        );
        assertTrue("cause: " + e, causeChainContains(e, "not a delegation-target backend"));
    }

    public void testNonDelegatablePredicateRejected() {
        // REGEXP has no serializer in this fixture → rejected even for the valid lucene namespace.
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        DelegationBlockList.create(clusterSettings, Settings.EMPTY, registry());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder().putList("analytics.delegation.lucene.blocked_predicates", "REGEXP").build()
            )
        );
        assertTrue("cause: " + e, causeChainContains(e, "not delegatable"));
    }

    public void testSeedRejectsInvalidNamespaceAtConstruction() {
        Settings settings = Settings.builder().putList("analytics.delegation.datafusion.blocked_predicates", "LIKE").build();
        expectThrows(IllegalArgumentException.class, () -> DelegationBlockList.create(clusterSettings(settings), settings, registry()));
    }

    /** The cluster-settings layer wraps the validator's IllegalArgumentException; search the chain. */
    private static boolean causeChainContains(Throwable t, String fragment) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(fragment)) {
                return true;
            }
        }
        return false;
    }
}
