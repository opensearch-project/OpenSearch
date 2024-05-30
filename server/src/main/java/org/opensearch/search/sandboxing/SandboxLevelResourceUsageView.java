/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.sandboxing.resourcetype.SandboxResourceType;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ExperimentalApi
public class SandboxLevelResourceUsageView {

    private final String sandboxId;
    private final Map<SandboxResourceType, Long> resourceUsage;
    private final List<Task> activeTasks;

    public SandboxLevelResourceUsageView(String sandboxId) {
        this.sandboxId = sandboxId;
        this.resourceUsage = new HashMap<>();
        this.activeTasks = new ArrayList<>();
    }

    public SandboxLevelResourceUsageView(String sandboxId, Map<SandboxResourceType, Long> resourceUsage, List<Task> activeTasks) {
        this.sandboxId = sandboxId;
        this.resourceUsage = resourceUsage;
        this.activeTasks = activeTasks;
    }

    public Map<SandboxResourceType, Long> getResourceUsageData() {
        return resourceUsage;
    }

    public List<Task> getActiveTasks() {
        return activeTasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SandboxLevelResourceUsageView that = (SandboxLevelResourceUsageView) o;
        return Objects.equals(sandboxId, that.sandboxId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sandboxId);
    }
}
