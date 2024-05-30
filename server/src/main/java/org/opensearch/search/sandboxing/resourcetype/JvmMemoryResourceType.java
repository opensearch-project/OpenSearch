/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandboxing.resourcetype;

import org.opensearch.tasks.Task;

public class JvmMemoryResourceType extends SandboxResourceType {
  @Override
  public long getResourceUsage(Task task) {
    return task.getTotalResourceStats().getMemoryInBytes();
  }
}
