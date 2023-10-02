/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.cluster.node.DiscoveryNode;

import java.net.UnknownHostException;

/**
 * Reference to a method that transports a parse request to an extension. By convention, this method takes
 * a category class used to identify the reader defined within the JVM that the extension is running on.
 * Additionally, this method takes in the extension's corresponding DiscoveryNode and a byte array (context) that the
 * extension's reader will be applied to.
 *
 * By convention the extensions' reader is a constructor that takes StreamInput as an argument for most classes and a static method for things like enums.
 * Classes will implement this via a constructor (or a static method in the case of enumerations), it's something that should
 * look like:
 * <pre><code>
 * public MyClass(final StreamInput in) throws IOException {
 * *     this.someValue = in.readVInt();
 *     this.someMap = in.readMapOfLists(StreamInput::readString, StreamInput::readString);
 * }
 * </code></pre>
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface ExtensionReader {

    /**
     * Transports category class, and StreamInput (context), to the extension identified by the Discovery Node
     *
     * @param extensionNode Discovery Node identifying the Extension
     * @param categoryClass Super class that the reader extends
     * @param context Some context to transport
     * @throws UnknownHostException if the extension node host IP address could not be determined
     */
    void parse(DiscoveryNode extensionNode, Class categoryClass, Object context) throws UnknownHostException;

}
