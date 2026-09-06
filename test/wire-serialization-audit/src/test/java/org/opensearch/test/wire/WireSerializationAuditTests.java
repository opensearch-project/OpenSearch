/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.wire;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class WireSerializationAuditTests extends OpenSearchTestCase {
    public void testFindsDirectAndInheritedEmptyWriters() throws IOException {
        final List<WireSerializationAudit.Finding> findings = scan(EmptyWriteable.class, EmptyChild.class, EmptyParent.class);

        assertThat(
            findings.stream().map(finding -> finding.className).toList(),
            containsInAnyOrder(EmptyWriteable.class.getName(), EmptyParent.class.getName(), EmptyChild.class.getName())
        );
        final WireSerializationAudit.Finding stateful = findings.stream()
            .filter(finding -> finding.className.equals(EmptyWriteable.class.getName()))
            .findFirst()
            .orElseThrow();
        assertThat(stateful.instanceFields, equalTo(1));
        assertThat(stateful.streamConstructor, equalTo("empty"));
        assertThat(stateful.classification(), equalTo("stateful-empty-reader"));
    }

    public void testIgnoresInheritedWriterThatEmitsBytes() throws IOException {
        assertThat(scan(WritingParent.class, WritingChild.class), empty());
    }

    private static List<WireSerializationAudit.Finding> scan(Class<?>... classes) throws IOException {
        final Map<String, ClassNode> classNodes = new HashMap<>();
        for (Class<?> clazz : classes) {
            try (InputStream input = clazz.getResourceAsStream("/" + clazz.getName().replace('.', '/') + ".class")) {
                final ClassNode classNode = new ClassNode(Opcodes.ASM9);
                new ClassReader(input).accept(classNode, 0);
                classNodes.put(classNode.name, classNode);
            }
        }
        return WireSerializationAudit.scan(classNodes);
    }

    static class EmptyWriteable implements Writeable {
        private int value;

        EmptyWriteable(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    static class EmptyParent implements Writeable {
        EmptyParent(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    static class EmptyChild extends EmptyParent {
        EmptyChild(StreamInput in) {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    static class WritingParent implements Writeable {
        WritingParent(StreamInput in) throws IOException {
            in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(1);
        }
    }

    static class WritingChild extends WritingParent {
        WritingChild(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
