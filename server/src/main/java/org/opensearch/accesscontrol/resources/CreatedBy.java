/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * This class is used to store information about the creator of a resource.
 * Concrete implementation will be provided by security plugin
 *
 * @opensearch.experimental
 */
public abstract class CreatedBy implements ToXContentFragment, NamedWriteable {

    // Non-default implementation provided by security plugin
    public static CreatedBy fromXContent(XContentParser parser) throws IOException {
        return null;
    }
}
