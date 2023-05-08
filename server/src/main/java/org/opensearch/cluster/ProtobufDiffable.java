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
 * Cluster state part, changes in which can be serialized
*
* @opensearch.internal
*/
public interface ProtobufDiffable<T> extends ProtobufWriteable {

    /**
     * Returns serializable object representing differences between this and previousState
    */
    ProtobufDiff<T> diff(T previousState);

}
