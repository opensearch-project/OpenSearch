/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration.model;

import java.util.Map;

public abstract class InternalUsersModel {

    public abstract boolean exists(String user);

    public abstract Map<String, String> getAttributes(String user);

    public abstract String getHash(String user);

}
