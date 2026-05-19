/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationInstructionNode;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FinalAggregateInstructionNode;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.JoinCapability;
import org.opensearch.analytics.spi.PartialAggregateInstructionNode;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;
import org.opensearch.analytics.spi.WindowCapability;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Base class for mock backends in planner tests.
 *
 * <p>Subclasses override only the capability methods relevant to their test.
 * {@link #getCapabilityProvider()} delegates to these overridable methods,
 * so anonymous subclasses only need to override what changes.
 */
abstract class MockBackend implements AnalyticsSearchBackendPlugin {

    @Override
    public final BackendCapabilityProvider getCapabilityProvider() {
        MockBackend self = this;
        return new BackendCapabilityProvider() {
            @Override
            public Set<EngineCapability> supportedEngineCapabilities() {
                return self.supportedEngineCapabilities();
            }

            @Override
            public Set<ScanCapability> scanCapabilities() {
                return self.scanCapabilities();
            }

            @Override
            public Set<FilterCapability> filterCapabilities() {
                return self.filterCapabilities();
            }

            @Override
            public Set<AggregateCapability> aggregateCapabilities() {
                return self.aggregateCapabilities();
            }

            @Override
            public Set<ProjectCapability> projectCapabilities() {
                return self.projectCapabilities();
            }

            @Override
            public Set<JoinCapability> joinCapabilities() {
                return self.joinCapabilities();
            }

            @Override
            public Set<WindowCapability> windowCapabilities() {
                return self.windowCapabilities();
            }

            @Override
            public Set<DelegationType> supportedDelegations() {
                return self.supportedDelegations();
            }

            @Override
            public Set<DelegationType> acceptedDelegations() {
                return self.acceptedDelegations();
            }

            @Override
            public Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                return self.scalarFunctionAdapters();
            }

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                return self.delegatedPredicateSerializers();
            }
        };
    }

    // Overridable capability methods — defaults return empty (no capability declared)
    protected Set<EngineCapability> supportedEngineCapabilities() {
        return Set.of();
    }

    protected Set<ScanCapability> scanCapabilities() {
        return Set.of();
    }

    protected Set<FilterCapability> filterCapabilities() {
        return Set.of();
    }

    protected Set<AggregateCapability> aggregateCapabilities() {
        return Set.of();
    }

    protected Set<ProjectCapability> projectCapabilities() {
        return Set.of();
    }

    protected Set<JoinCapability> joinCapabilities() {
        return Set.of();
    }

    protected Set<WindowCapability> windowCapabilities() {
        return Set.of();
    }

    protected Set<DelegationType> supportedDelegations() {
        return Set.of();
    }

    protected Set<DelegationType> acceptedDelegations() {
        return Set.of();
    }

    protected Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
        return Map.of();
    }

    protected Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
        return Map.of();
    }

    @Override
    public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        return new FragmentInstructionHandlerFactory() {
            @Override
            public Optional<InstructionNode> createShardScanNode() {
                return Optional.of(new ShardScanInstructionNode());
            }

            @Override
            public Optional<InstructionNode> createFilterDelegationNode(
                FilterTreeShape treeShape,
                int delegatedPredicateCount,
                List<DelegatedExpression> delegatedExpressions
            ) {
                return Optional.of(new FilterDelegationInstructionNode(treeShape, delegatedPredicateCount, delegatedExpressions));
            }

            @Override
            public Optional<InstructionNode> createShardScanWithDelegationNode(FilterTreeShape treeShape, int delegatedPredicateCount) {
                return Optional.of(new ShardScanWithDelegationInstructionNode(treeShape, delegatedPredicateCount));
            }

            @Override
            public Optional<InstructionNode> createPartialAggregateNode() {
                return Optional.of(new PartialAggregateInstructionNode());
            }

            @Override
            public Optional<InstructionNode> createFinalAggregateNode() {
                return Optional.of(new FinalAggregateInstructionNode());
            }

            @Override
            public FragmentInstructionHandler<?> createHandler(InstructionNode node) {
                throw new UnsupportedOperationException("Mock backend does not execute instructions");
            }
        };
    }
}
