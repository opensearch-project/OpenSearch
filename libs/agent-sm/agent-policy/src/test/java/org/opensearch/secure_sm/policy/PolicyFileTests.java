/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import org.junit.Test;

import java.io.FilePermission;
import java.io.IOException;
import java.security.PermissionCollection;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class PolicyFileTests {
    @SuppressWarnings("removal")
    @Test
    public void testPolicy() throws IOException, PolicyParser.ParsingException {
        final java.security.Policy policy = new PolicyFile(getClass().getResource("/test.policy"));
        final PermissionCollection permissions = policy.getPermissions(getClass().getProtectionDomain());

        final List<FilePermission> filePermissions = permissions.elementsAsStream()
            .filter(FilePermission.class::isInstance)
            .map(FilePermission.class::cast)
            .toList();
        assertThat(filePermissions, not(empty()));

        assertThat(filePermissions.stream().filter(p -> p.getName().equalsIgnoreCase("/sys/fs/cgroup/memory")).count(), equalTo(1L));
        assertThat(filePermissions.stream().filter(p -> p.getName().contains("/lib/security/cacerts")).count(), equalTo(1L));
    }
}
