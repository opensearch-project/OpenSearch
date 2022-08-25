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

public interface Principal {
    UUID getId();

    List<String> getSchemas();

    String getUserName();

    Map<String, String> getMetaData();

}
