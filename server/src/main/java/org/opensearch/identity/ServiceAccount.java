/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.security.Principal;
import java.util.List;

/**
 * This interface defines a ServiceAccount.
 *
 * A ServiceAccount is a principal which can retrieve its associated permissions.
 */
public interface ServiceAccount extends Principal {

    public List<String> getPermissions();
}
