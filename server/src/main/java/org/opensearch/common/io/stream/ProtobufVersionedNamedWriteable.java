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

import org.opensearch.Version;

/**
 * A {@link NamedWriteable} that has a minimum version associated with it.
*
* @opensearch.internal
*/
public interface ProtobufVersionedNamedWriteable extends ProtobufNamedWriteable {

    /**
     * Returns the name of the writeable object
    */
    String getWriteableName();

    /**
     * The minimal version of the recipient this object can be sent to
    */
    Version getMinimalSupportedVersion();
}
