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

package org.opensearch.common.io.stream;

/**
 * A {@link Writeable} object identified by its name.
* To be used for arbitrary serializable objects (e.g. queries); when reading them, their name tells
* which specific object needs to be created.
*
* @opensearch.internal
*/
public interface ProtobufNamedWriteable extends ProtobufWriteable {

    /**
     * Returns the name of the writeable object
    */
    String getWriteableName();
}
