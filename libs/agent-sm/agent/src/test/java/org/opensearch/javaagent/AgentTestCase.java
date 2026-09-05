/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;
import org.junit.BeforeClass;

import java.io.FilePermission;
import java.nio.file.Path;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Set;

public abstract class AgentTestCase {
    private static String getJacocoDirPermissionPath() {
        return Path.of(System.getProperty("user.dir")).resolve("../../jacoco").normalize() + "/-";
    }

    @SuppressWarnings("removal")
    @BeforeClass
    public static void setUp() {
        AgentPolicy.setPolicy(new Policy() {
            @Override
            public PermissionCollection getPermissions(ProtectionDomain domain) {
                Permissions permissions = new Permissions();
                permissions.add(new FilePermission(getJacocoDirPermissionPath(), "write"));
                return permissions;
            }

            @Override
            public boolean implies(ProtectionDomain domain, Permission permission) {
                final PermissionCollection pc = getPermissions(domain);

                if (pc == null) {
                    return false;
                }

                return pc.implies(permission);
            }
        },
            Set.of(),
            Set.of(),
            (caller, chain) -> caller.getName().equalsIgnoreCase("worker.org.gradle.process.internal.worker.GradleWorkerMain")
        );
    }
}
