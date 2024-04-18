/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;

/**
 * Base class for outbound data as a message.
 * Different implementations are used for different protocols.
 *
 * @opensearch.internal
 */
public interface ProtocolOutboundMessage {

    BytesReference serialize(BytesStreamOutput bytesStream) throws IOException;

}
