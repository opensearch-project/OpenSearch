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
  @Override
  public boolean equals(Object obj) {
    return obj instanceof JvmMemoryResourceType;
  }

  @Override
  public int hashCode() {
    return "JVM".hashCode();
  }
}
