/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.cluster;

import org.opensearch.common.io.stream.ProtobufWriteable;

/**
 * Represents difference between states of cluster state parts
*
* @opensearch.internal
*/
public interface ProtobufDiff<T> extends ProtobufWriteable {

    /**
     * Applies difference to the specified part and returns the resulted part
    */
    T apply(T part);
}
