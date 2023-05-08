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

import org.opensearch.Version;
import org.opensearch.common.io.stream.ProtobufNamedWriteable;

/**
 * Diff that also support NamedWriteable interface
*
* @opensearch.internal
*/
public interface ProtobufNamedDiff<T extends ProtobufDiffable<T>> extends ProtobufDiff<T>, ProtobufNamedWriteable {
    /**
     * The minimal version of the recipient this custom object can be sent to
    */
    default Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumIndexCompatibilityVersion();
    }

}
