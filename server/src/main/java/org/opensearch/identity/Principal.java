/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A principal identifier (user or a group) for extensions requests
 * The implementation should be serializable/deserializable
*/

// TODO: Determine how this principal can be shared between threads
public interface Principal {
    // TODO: Determine how to handle blank or null users/groups (if there is such possibility)
    /* A unique identifier for each entity */
    UUID getId();

    /* Contains principal type definition */
    List<String> getSchemas();

    /* A non-unique human readable identifier */
    String getUserName();

    /* Contains metadata relevant to entity*/
    Map<String, String> getMetaData();

}
