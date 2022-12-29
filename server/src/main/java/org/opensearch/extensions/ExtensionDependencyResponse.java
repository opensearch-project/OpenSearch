/*
* Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

 package org.opensearch.extensions;

 import java.io.IOException;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.Objects;
 import org.opensearch.common.io.stream.StreamInput;
 import org.opensearch.common.io.stream.StreamOutput;
 import org.opensearch.transport.TransportResponse;
 
 /**
  * The response for getting the Extension Dependency.
  *
  * @opensearch.internal
  */
 public class ExtensionDependencyResponse extends TransportResponse {
     private List<DiscoveryExtensionNode> dependencies;
 
     public ExtensionDependencyResponse(List<DiscoveryExtensionNode> dependencies) {
         this.dependencies = dependencies;
     }
 
     public ExtensionDependencyResponse(StreamInput in) throws IOException {
         int size = in.readVInt();
         dependencies = new ArrayList<>(size);
         for (int i = 0; i < size; i++) {
             dependencies.add(new DiscoveryExtensionNode(in));
         }
     }
 
     @Override
     public void writeTo(StreamOutput out) throws IOException {
         out.writeVInt(dependencies.size());
         for (DiscoveryExtensionNode dependency : dependencies) {
             dependency.writeTo(out);
         }
     }
 
     public List<DiscoveryExtensionNode> getExtensionDependency() {
         return dependencies;
     }
 
     @Override
     public String toString() {
         return "ExtensionDependencyResponse{extensiondependency=" + dependencies.toString() + '}';
     }
 
     @Override
     public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ExtensionDependencyResponse that = (ExtensionDependencyResponse) o;
         return Objects.equals(dependencies, that.dependencies);
     }
 
     @Override
     public int hashCode() {
         return Objects.hash(dependencies);
     }
 }
 