/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import java.util.List;

public record GrantEntry(String codeBase, List<PermissionEntry> permissionEntries) {
}
