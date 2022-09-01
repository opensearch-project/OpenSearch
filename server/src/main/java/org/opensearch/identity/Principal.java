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

import org.opensearch.common.io.stream.NamedWriteable;

import java.io.Serializable;

/**
 * A principal identifier (user or a group) for extensions requests
 * It is a minimal user representation per SCIM: https://www.simplecloud.info/
 * The implementation should be serializable/deserializable
*/

// TODO: Determine how this principal can be shared between threads and
// the effects of extending NamedWriteable class
public interface Principal extends Serializable, NamedWriteable {
    // TODO: Determine how to handle blank or null users/groups (if there is such possibility)
    /* A unique identifier for each entity */
    String getPrincipalIdentifier();

    /* A non-unique human readable identifier */
    String getUsername();
}
