/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing;

import org.opensearch.cluster.metadata.Sandbox;

import java.util.List;
import java.util.stream.Collectors;

public class DefaultSandboxPruner implements SandboxPruner {
    private List<Sandbox> activeSandboxes;
    private List<String> toDeleteSandboxes;
    private TaskData taskData;

    @Override
    public void pruneSandboxes() {
        toDeleteSandboxes = toDeleteSandboxes.stream().filter(this::hasUnfinishedTasks).collect(Collectors.toList());
    }

    @Override
    public void deleteSandbox(String sandboxId) {
        // should be called from the API and we need to delete from cluster metadata
        // TODO may be we need to build a listener interface instead
        if (hasUnfinishedTasks(sandboxId)) {
            toDeleteSandboxes.add(sandboxId);
        }
        // remove this sandbox from the active sandboxes
        activeSandboxes = activeSandboxes.stream()
            .filter(resourceLimitGroup -> !resourceLimitGroup.getName().equals(sandboxId))
            .collect(Collectors.toList());
    }

    private boolean hasUnfinishedTasks(String sandboxName) {
        return !taskData.getTasksBySandbox().get(sandboxName).isEmpty();
    }

    public List<Sandbox> getActiveSandboxes() {
        return activeSandboxes;
    }

    public void setActiveSandboxes(List<Sandbox> activeSandboxes) {
        this.activeSandboxes = activeSandboxes;
    }

    public List<String> getToDeleteSandboxes() {
        return toDeleteSandboxes;
    }

    public void setToDeleteSandboxes(List<String> toDeleteSandboxes) {
        this.toDeleteSandboxes = toDeleteSandboxes;
    }

    public TaskData getTaskData() {
        return taskData;
    }

    public void setTaskData(TaskData taskData) {
        this.taskData = taskData;
    }
}
