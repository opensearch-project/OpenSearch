/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.cluster;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Cluster Name
*
* @opensearch.internal
*/
public class ProtobufClusterName implements ProtobufWriteable {

    public static final Setting<ProtobufClusterName> CLUSTER_NAME_SETTING = new Setting<>("cluster.name", "opensearch", (s) -> {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("[cluster.name] must not be empty");
        }
        if (s.contains(":")) {
            throw new IllegalArgumentException("[cluster.name] must not contain ':'");
        }
        return new ProtobufClusterName(s);
    }, Setting.Property.NodeScope);

    public static final ProtobufClusterName DEFAULT = CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY);

    private final String value;

    public ProtobufClusterName(CodedInputStream input) throws IOException {
        this(input.readString());
    }

    public ProtobufClusterName(String value) {
        this.value = value.intern();
    }

    public String value() {
        return this.value;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeStringNoTag(value);
    }

    public Predicate<ProtobufClusterName> getEqualityPredicate() {
        return new Predicate<ProtobufClusterName>() {
            @Override
            public boolean test(ProtobufClusterName o) {
                return ProtobufClusterName.this.equals(o);
            }

            @Override
            public String toString() {
                return "local cluster name [" + ProtobufClusterName.this.value() + "]";
            }
        };
    }
}
