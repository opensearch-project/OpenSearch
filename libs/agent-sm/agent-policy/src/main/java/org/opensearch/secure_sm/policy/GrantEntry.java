/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;

public class GrantEntry {
    public String codeBase;
    private final LinkedList<PermissionEntry> permissionEntries = new LinkedList<>();

    public void add(PermissionEntry entry) {
        permissionEntries.add(entry);
    }

    public Enumeration<PermissionEntry> permissionElements() {
        return Collections.enumeration(permissionEntries);
    }

    public void write(PrintWriter out) {
        out.print("grant");
        if (codeBase != null) {
            out.print(" Codebase \"");
            out.print(codeBase);
            out.print("\"");
        }
        out.println(" {");
        for (PermissionEntry pe : permissionEntries) {
            out.print("  permission ");
            out.print(pe.permission);
            if (pe.name != null) {
                out.print(" \"");
                out.print(pe.name);
                out.print("\"");
            }
            if (pe.action != null) {
                out.print(", \"");
                out.print(pe.action);
                out.print("\"");
            }
            out.println(";");
        }
        out.println("};");
    }
}
