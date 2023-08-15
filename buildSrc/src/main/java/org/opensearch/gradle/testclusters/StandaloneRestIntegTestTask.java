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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.gradle.testclusters;

import groovy.lang.Closure;

import org.opensearch.gradle.FileSystemOperationsAware;
import org.opensearch.gradle.test.Fixture;
import org.opensearch.gradle.util.GradleUtils;
import org.gradle.api.Task;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.internal.BuildServiceProvider;
import org.gradle.api.services.internal.BuildServiceRegistryInternal;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
import org.gradle.api.tasks.WorkResult;
import org.gradle.api.tasks.testing.Test;
import org.gradle.internal.resources.ResourceLock;
import org.gradle.internal.resources.SharedResource;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Customized version of Gradle {@link Test} task which tracks a collection of {@link OpenSearchCluster} as a task input. We must do this
 * as a custom task type because the current {@link org.gradle.api.tasks.TaskInputs} runtime API does not have a way to register
 * {@link Nested} inputs.
 */
@CacheableTask
public class StandaloneRestIntegTestTask extends Test implements TestClustersAware, FileSystemOperationsAware {

    private Collection<OpenSearchCluster> clusters = new HashSet<>();
    private Closure<Void> beforeStart;

    public StandaloneRestIntegTestTask() {
        this.getOutputs()
            .doNotCacheIf(
                "Caching disabled for this task since it uses a cluster shared by other tasks",
                /*
                 * Look for any other tasks which use the same cluster as this task. Since tests often have side effects for the cluster
                 * they execute against, this state can cause issues when trying to cache tests results of tasks that share a cluster. To
                 * avoid any undesired behavior we simply disable the cache if we detect that this task uses a cluster shared between
                 * multiple tasks.
                 */
                t -> getProject().getTasks()
                    .withType(StandaloneRestIntegTestTask.class)
                    .stream()
                    .filter(task -> task != this)
                    .anyMatch(task -> Collections.disjoint(task.getClusters(), getClusters()) == false)
            );

        this.getOutputs()
            .doNotCacheIf(
                "Caching disabled for this task since it is configured to preserve data directory",
                // Don't cache the output of this task if it's not running from a clean data directory.
                t -> getClusters().stream().anyMatch(cluster -> cluster.isPreserveDataDir())
            );
    }

    // Hook for executing any custom logic before starting the task.
    public void setBeforeStart(Closure<Void> closure) {
        beforeStart = closure;
    }

    @Override
    public void beforeStart() {
        if (beforeStart != null) {
            beforeStart.call(this);
        }
    }

    @Override
    public int getMaxParallelForks() {
        return 1;
    }

    @Nested
    @Override
    public Collection<OpenSearchCluster> getClusters() {
        return clusters;
    }

    @Override
    @Internal
    public List<ResourceLock> getSharedResources() {
        List<ResourceLock> locks = new ArrayList<>(super.getSharedResources());
        BuildServiceRegistryInternal serviceRegistry = getServices().get(BuildServiceRegistryInternal.class);
        Provider<TestClustersThrottle> throttleProvider = GradleUtils.getBuildService(
            serviceRegistry,
            TestClustersPlugin.THROTTLE_SERVICE_NAME
        );
        final SharedResource resource = getSharedResource(serviceRegistry, throttleProvider);

        int nodeCount = clusters.stream().mapToInt(cluster -> cluster.getNodes().size()).sum();
        if (nodeCount > 0) {
            locks.add(getResourceLock(resource, Math.min(nodeCount, resource.getMaxUsages())));
        }

        return Collections.unmodifiableList(locks);
    }

    private SharedResource getSharedResource(
        BuildServiceRegistryInternal serviceRegistry,
        Provider<TestClustersThrottle> throttleProvider
    ) {
        try {
            try {
                // The forService(Provider) is used by Gradle pre-8.0
                return (SharedResource) MethodHandles.publicLookup()
                    .findVirtual(
                        BuildServiceRegistryInternal.class,
                        "forService",
                        MethodType.methodType(SharedResource.class, Provider.class)
                    )
                    .bindTo(serviceRegistry)
                    .invokeExact(throttleProvider);
            } catch (NoSuchMethodException | IllegalAccessException ex) {
                // The forService(BuildServiceProvider) is used by Gradle post-8.0
                return (SharedResource) MethodHandles.publicLookup()
                    .findVirtual(
                        BuildServiceRegistryInternal.class,
                        "forService",
                        MethodType.methodType(SharedResource.class, BuildServiceProvider.class)
                    )
                    .bindTo(serviceRegistry)
                    .invokeExact((BuildServiceProvider<TestClustersThrottle, ?>) throttleProvider);
            }
        } catch (Throwable ex) {
            throw new IllegalStateException("Unable to find suitable BuildServiceRegistryInternal::forService", ex);
        }
    }

    private ResourceLock getResourceLock(SharedResource resource, int nUsages) {
        try {
            try {
                // The getResourceLock(int) is used by Gradle pre-7.5
                return (ResourceLock) MethodHandles.publicLookup()
                    .findVirtual(SharedResource.class, "getResourceLock", MethodType.methodType(ResourceLock.class, int.class))
                    .bindTo(resource)
                    .invokeExact(nUsages);
            } catch (NoSuchMethodException | IllegalAccessException ex) {
                // The getResourceLock() is used by Gradle post-7.4
                return (ResourceLock) MethodHandles.publicLookup()
                    .findVirtual(SharedResource.class, "getResourceLock", MethodType.methodType(ResourceLock.class))
                    .bindTo(resource)
                    .invokeExact();
            }
        } catch (Throwable ex) {
            throw new IllegalStateException("Unable to find suitable ResourceLock::getResourceLock", ex);
        }
    }

    @Override
    public Task dependsOn(Object... dependencies) {
        super.dependsOn(dependencies);
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture) dependency).getStopTask());
            }
        }
        return this;
    }

    @Override
    public void setDependsOn(Iterable<?> dependencies) {
        super.setDependsOn(dependencies);
        for (Object dependency : dependencies) {
            if (dependency instanceof Fixture) {
                finalizedBy(((Fixture) dependency).getStopTask());
            }
        }
    }

    public WorkResult delete(Object... objects) {
        return getFileSystemOperations().delete(d -> d.delete(objects));
    }
}
