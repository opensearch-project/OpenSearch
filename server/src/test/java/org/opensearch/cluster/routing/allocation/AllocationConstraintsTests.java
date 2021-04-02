package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllocationConstraintsTests extends OpenSearchAllocationTestCase {

    public void testSettings() {
        Settings.Builder settings = Settings.builder();
        ClusterSettings service = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);
        assertFalse(allocator.isConstraintFrameworkEnabled());
        assertNull(allocator.getConstraints());

        // Changing other balancer settings should not affect constraint settings
        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        service.applySettings(settings.build());
        assertFalse(allocator.isConstraintFrameworkEnabled());
        assertNull(allocator.getConstraints());
        assertEquals(0.2, allocator.getIndexBalance(), 0.01);
        assertEquals(0.3, allocator.getShardBalance(), 0.01);
        assertEquals(2.0, allocator.getThreshold(), 0.01);

        // Toggle allConstraintsDisabled setting
        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.CONSTRAINT_FRAMEWORK_ENABLED_SETTING.getKey(), false);
        service.applySettings(settings.build());
        assertFalse(allocator.isConstraintFrameworkEnabled());
        assertNull(allocator.getConstraints());

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.CONSTRAINT_FRAMEWORK_ENABLED_SETTING.getKey(), true);
        service.applySettings(settings.build());
        assertTrue(allocator.isConstraintFrameworkEnabled());
        assertEquals(allocator.getConstraints().getIndexShardsPerNodeConstraint(), AllocationConstraints.ConstraintMode.NONE);

        // Enable constraint using dynamic settings
        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.CONSTRAINT_FRAMEWORK_ENABLED_SETTING.getKey(), true)
                .put(
                    AllocationConstraints.INDEX_SHARDS_PER_NODE_CONSTRAINT_SETTING.getKey(),
                    AllocationConstraints.ConstraintMode.UNASSIGNED.toString()
                );
        service.applySettings(settings.build());
        assertEquals(allocator.getConstraints().getIndexShardsPerNodeConstraint(), AllocationConstraints.ConstraintMode.UNASSIGNED);

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.CONSTRAINT_FRAMEWORK_ENABLED_SETTING.getKey(), true)
                .put(
                    AllocationConstraints.INDEX_SHARDS_PER_NODE_CONSTRAINT_SETTING.getKey(),
                    AllocationConstraints.ConstraintMode.NONE.toString()
                );
        service.applySettings(settings.build());
        assertEquals(allocator.getConstraints().getIndexShardsPerNodeConstraint(), AllocationConstraints.ConstraintMode.NONE);
    }

    /**
     * Test constraint evaluation logic when with different values of ConstraintMode
     * for IndexShardPerNode constraint satisfied and breached.
     */
    public void testIndexShardsPerNodeConstraint() {
        BalancedShardsAllocator.Balancer balancer = mock(BalancedShardsAllocator.Balancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        when(balancer.avgShardsPerNode(anyString())).thenReturn(3.33f);
        when(node.getNodeId()).thenReturn("test-node");

        /* Constraint breached
         */

        when(node.numShards(anyString())).thenReturn(4);

        AllocationConstraints constraints = new AllocationConstraints(AllocationConstraints.ConstraintMode.UNASSIGNED);
        // unassigned only
        assertEquals(constraints.CONSTRAINT_WEIGHT, constraints.weight(balancer, node, "index",
                AllocationConstraints.ConstraintMode.UNASSIGNED));

        // none enabled
        constraints = new AllocationConstraints(AllocationConstraints.ConstraintMode.NONE);
        assertEquals(0, constraints.weight(balancer, node, "index",
                AllocationConstraints.ConstraintMode.UNASSIGNED));

        /* Constraint not breached
         */

        when(node.numShards(anyString())).thenReturn(3);

        // unassigned only
        constraints = new AllocationConstraints(AllocationConstraints.ConstraintMode.UNASSIGNED);
        assertEquals(0, constraints.weight(balancer, node, "index",
                AllocationConstraints.ConstraintMode.UNASSIGNED));


        // none enabled
        constraints = new AllocationConstraints(AllocationConstraints.ConstraintMode.NONE);
        assertEquals(0, constraints.weight(balancer, node, "index",
                AllocationConstraints.ConstraintMode.UNASSIGNED));
    }

    /**
     * Verify constraint weight function is not called if all constraints are disabled.
     */
    public void testAllConstraintsDisabled() {
        BalancedShardsAllocator.Balancer balancer = mock(BalancedShardsAllocator.Balancer.class);
        BalancedShardsAllocator.ModelNode node = mock(BalancedShardsAllocator.ModelNode.class);
        BalancedShardsAllocator.WeightFunction wf;

        // Mock balancer params to breach constraint
        when(node.numShards()).thenReturn(10);
        when(balancer.avgShardsPerNode()).thenReturn(6f);
        when(node.numShards(anyString())).thenReturn(4);
        when(balancer.avgShardsPerNode(anyString())).thenReturn(0.33f);
        when(node.getNodeId()).thenReturn("test-node");

        wf = new BalancedShardsAllocator.WeightFunction(0.45f, 0.55f, null);
        float balancerWt = wf.weight(balancer, node, "index", AllocationConstraints.ConstraintMode.UNASSIGNED);

        AllocationConstraints constraints = new AllocationConstraints(AllocationConstraints.ConstraintMode.UNASSIGNED);
        wf = new BalancedShardsAllocator.WeightFunction(0.45f, 0.55f, constraints);
        float constraintWt = wf.weight(balancer, node, "index", AllocationConstraints.ConstraintMode.UNASSIGNED);

        assertEquals(constraints.CONSTRAINT_WEIGHT, constraintWt - balancerWt, 0.00001f);
    }
}
