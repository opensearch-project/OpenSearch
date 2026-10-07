/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.testclusters;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import java.util.List;
import java.util.stream.Collectors;

public class OpenSearchClusterTests extends GradleUnitTestCase {

    public void testNodesAreNumberedFromZeroByDefault() {
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setNumberOfNodes(2);

        assertEquals(List.of("runTask-0", "runTask-1"), names(cluster));
        assertEquals("runTask-0", cluster.getFirstNode().getName());
    }

    public void testNodesAreNumberedFromTheFirstNodeIndex() {
        // So that a second build's nodes have their own names, and so their own working directories.
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setFirstNodeIndex(2);
        cluster.setNumberOfNodes(2);

        assertEquals(List.of("runTask-2", "runTask-3"), names(cluster));
        assertEquals("runTask-2", cluster.getFirstNode().getName());
    }

    public void testASingleNodeCanBeNumberedFromTheFirstNodeIndex() {
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setFirstNodeIndex(4);

        assertEquals(List.of("runTask-4"), names(cluster));
    }

    public void testTheNumberAndNumberingOfNodesCanBeSetInEitherOrder() {
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setNumberOfNodes(2);
        cluster.setFirstNodeIndex(2);

        assertEquals(List.of("runTask-2", "runTask-3"), names(cluster));
    }

    public void testASingleNodeCanBeAskedForExplicitly() {
        // It used to be an error to "shrink" the eagerly created first node's cluster to the one node it had.
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setNumberOfNodes(1);

        assertEquals(List.of("runTask-0"), names(cluster));
    }

    public void testConfigurationSetBeforeTheNodesExistReachesEveryNode() {
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setting("cluster.routing.allocation.enable", "primaries");
        cluster.systemProperty("tests.example", "yes");
        cluster.setNumberOfNodes(3);

        for (OpenSearchNode node : cluster.getNodes()) {
            assertEquals(node.getName(), "primaries", String.valueOf(node.settings.get("cluster.routing.allocation.enable")));
        }
        assertEquals(3, cluster.getNodes().size());
    }

    public void testUsingAClusterDoesNotCreateItsNodes() {
        // useCluster runs as soon as a task is configured, often before the cluster's own configuration is finished.
        Project project = ProjectBuilder.builder().build();
        OpenSearchCluster cluster = cluster(project);
        RunTask run = project.getTasks().create("run", RunTask.class);
        run.useCluster(project, cluster);

        cluster.setFirstNodeIndex(2);
        cluster.setNumberOfNodes(2);

        assertEquals(List.of("runTask-2", "runTask-3"), names(cluster));
    }

    public void testTheNodesCannotBeRenumberedOnceTheyExist() {
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.getNodes();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> cluster.setFirstNodeIndex(2));
        assertTrue(e.getMessage(), e.getMessage().contains("already created, by"));
        assertTrue("and says what created them: " + e.getMessage(), e.getMessage().contains(OpenSearchClusterTests.class.getName()));
    }

    public void testTheNodesAreCreatedWhenTheProjectHasBeenEvaluated() {
        // So that they, and the distributions and configurations they create, exist before Gradle plans any task with them.
        Project project = ProjectBuilder.builder().build();
        OpenSearchCluster cluster = cluster(project);
        cluster.setNumberOfNodes(2);

        ((org.gradle.api.internal.project.ProjectInternal) project).evaluate();

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> cluster.setFirstNodeIndex(2));
        assertTrue(e.getMessage(), e.getMessage().contains("the end of"));
        assertEquals(List.of("runTask-0", "runTask-1"), names(cluster));
    }

    public void testAClusterCanStillGrowOnceItsNodesExist() {
        // As it always could, so a build script that reads the nodes before saying how many there are keeps working.
        OpenSearchCluster cluster = cluster(ProjectBuilder.builder().build());
        cluster.setting("cluster.routing.allocation.enable", "primaries");
        assertEquals(1, cluster.getNodes().size());

        cluster.setNumberOfNodes(3);

        assertEquals(List.of("runTask-0", "runTask-1", "runTask-2"), names(cluster));
        for (OpenSearchNode node : cluster.getNodes()) {
            assertEquals(node.getName(), "primaries", String.valueOf(node.settings.get("cluster.routing.allocation.enable")));
        }
        expectThrows(IllegalArgumentException.class, () -> cluster.setNumberOfNodes(2));
    }

    public void testZonesContinueFromTheFirstNodeIndex() {
        Project project = ProjectBuilder.builder().build();
        project.getExtensions().getExtraProperties().set("numZones", "2");
        OpenSearchCluster cluster = cluster(project);
        cluster.setNumberOfZones(2);
        cluster.setFirstNodeIndex(3);
        cluster.setNumberOfNodes(2);

        // Round-robin by node index, so runTask-3 is in the same zone as runTask-1 would be.
        assertEquals(List.of("zone-2", "zone-1"), cluster.getNodes().stream().map(OpenSearchNode::getZone).collect(Collectors.toList()));
    }

    public void testRunTaskStartsPortsAtTheFirstNodeIndex() {
        Project project = ProjectBuilder.builder().build();
        OpenSearchCluster cluster = cluster(project);
        cluster.setFirstNodeIndex(2);
        cluster.setNumberOfNodes(2);
        RunTask run = project.getTasks().create("run", RunTask.class);
        run.useCluster(project, cluster);

        run.beforeStart();

        List<OpenSearchNode> nodes = List.copyOf(cluster.getNodes());
        assertEquals(List.of("9202", "9203"), nodes.stream().map(OpenSearchNode::getHttpPort).collect(Collectors.toList()));
        assertEquals(List.of("9302", "9303"), nodes.stream().map(OpenSearchNode::getTransportPort).collect(Collectors.toList()));
    }

    public void testRunTaskStartsPortsAtTheDefaultsWithoutAnOffset() {
        Project project = ProjectBuilder.builder().build();
        OpenSearchCluster cluster = cluster(project);
        cluster.setNumberOfNodes(2);
        RunTask run = project.getTasks().create("run", RunTask.class);
        run.useCluster(project, cluster);

        run.beforeStart();

        List<OpenSearchNode> nodes = List.copyOf(cluster.getNodes());
        assertEquals(List.of("9200", "9201"), nodes.stream().map(OpenSearchNode::getHttpPort).collect(Collectors.toList()));
        assertEquals(List.of("9300", "9301"), nodes.stream().map(OpenSearchNode::getTransportPort).collect(Collectors.toList()));
    }

    public void testRunTaskWithPortOverridesInMultiNode() {
        Project project = ProjectBuilder.builder().build();
        OpenSearchCluster cluster = cluster(project);
        cluster.setNumberOfNodes(3);
        RunTask run = project.getTasks().create("run", RunTask.class);
        run.useCluster(project, cluster);
        System.setProperty("tests.opensearch.http.port", "10000");
        System.setProperty("tests.opensearch.transport.port", "10100");

        try {
            run.beforeStart();

            List<OpenSearchNode> nodes = List.copyOf(cluster.getNodes());
            assertEquals(List.of("10000", "10001", "10002"), nodes.stream().map(OpenSearchNode::getHttpPort).collect(Collectors.toList()));
            assertEquals(
                List.of("10100", "10101", "10102"),
                nodes.stream().map(OpenSearchNode::getTransportPort).collect(Collectors.toList())
            );
            // Seed hosts should point to first node's actual transport port
            for (OpenSearchNode node : nodes) {
                assertEquals("127.0.0.1:10100", node.settings.get("discovery.seed_hosts"));
            }
        } finally {
            System.clearProperty("tests.opensearch.http.port");
            System.clearProperty("tests.opensearch.transport.port");
        }
    }

    @SuppressWarnings("unchecked")
    private static OpenSearchCluster cluster(Project project) {
        project.getPlugins().apply(TestClustersPlugin.class);
        NamedDomainObjectContainer<OpenSearchCluster> clusters = (NamedDomainObjectContainer<OpenSearchCluster>) project.getExtensions()
            .getByName(TestClustersPlugin.EXTENSION_NAME);
        return clusters.create("runTask");
    }

    private static List<String> names(OpenSearchCluster cluster) {
        return cluster.getNodes().stream().map(OpenSearchNode::getName).collect(Collectors.toList());
    }
}
