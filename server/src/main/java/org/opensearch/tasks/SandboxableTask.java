/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

/**
 * This interface is used to identify sandboxable tasks, which can be tracked and used under a sandbox
 */
public interface SandboxableTask {
    public void setSandboxId(String sandboxId);

    public String getSandboxId();
}
