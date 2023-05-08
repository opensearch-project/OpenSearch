/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.node;

import org.opensearch.common.io.stream.ProtobufWriteable;

/**
 * Node reporting service
*
* @opensearch.internal
*/
public interface ProtobufReportingService<I extends ProtobufReportingService.ProtobufInfo> {
    I info();

    /**
     * Information interface.
    *
    * @opensearch.internal
    */
    interface ProtobufInfo extends ProtobufWriteable {

    }
}
