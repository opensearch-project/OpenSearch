/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.secure_sm.policy;

import java.io.PrintWriter;
import java.util.Objects;

public class PermissionNode {
    public String permission;
    public String name;
    public String action;

    @Override
    public int hashCode() {
        return Objects.hash(permission, name, action);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;

        return obj instanceof PermissionNode that
            && Objects.equals(this.permission, that.permission)
            && Objects.equals(this.name, that.name)
            && Objects.equals(this.action, that.action);
    }

    public void write(PrintWriter out) {
        out.print("permission ");
        out.print(permission);
        if (name != null) {
            out.print(" \"");
            out.print(name.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\""));
            out.print('"');
        }
        if (action != null) {
            out.print(", \"");
            out.print(action);
            out.print('"');
        }
        out.println(";");
    }
}
