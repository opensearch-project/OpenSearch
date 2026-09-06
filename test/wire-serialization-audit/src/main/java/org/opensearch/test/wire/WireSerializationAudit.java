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
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.VarInsnNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reports wire serializers that emit no bytes. */
public class WireSerializationAudit {
    static final String STREAM_INPUT = "org/opensearch/core/common/io/stream/StreamInput";
    static final String STREAM_OUTPUT = "org/opensearch/core/common/io/stream/StreamOutput";
    static final String WRITE_TO_DESCRIPTOR = "(L" + STREAM_OUTPUT + ";)V";
    static final String STREAM_CONSTRUCTOR_DESCRIPTOR = "(L" + STREAM_INPUT + ";)V";

    public static void main(String[] args) throws Exception {
        final List<Path> roots = new ArrayList<>();
        for (String arg : args) {
            roots.add(Paths.get(arg));
        }

        System.out.println("class\tline\tinstance_fields\tstream_constructor\tclassification");
        for (Finding finding : scan(roots)) {
            System.out.println(
                finding.className
                    + "\t"
                    + finding.line
                    + "\t"
                    + finding.instanceFields
                    + "\t"
                    + finding.streamConstructor
                    + "\t"
                    + finding.classification()
            );
        }
    }

    static List<Finding> scan(Collection<Path> roots) throws IOException {
        final Map<String, ClassNode> classes = new HashMap<>();
        for (Path root : roots) {
            if (Files.isDirectory(root) == false) {
                continue;
            }
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().endsWith(".class")) {
                        try (InputStream input = Files.newInputStream(file)) {
                            final ClassNode classNode = new ClassNode(Opcodes.ASM9);
                            new ClassReader(input).accept(classNode, 0);
                            classes.put(classNode.name, classNode);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
        return scan(classes);
    }

    static List<Finding> scan(Map<String, ClassNode> classes) {
        final List<Finding> findings = new ArrayList<>();
        for (ClassNode classNode : classes.values()) {
            if ((classNode.access & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_INTERFACE)) != 0) {
                continue;
            }
            final MethodNode writeTo = findMethod(classNode, "writeTo", WRITE_TO_DESCRIPTOR);
            if (writeTo != null
                && (writeTo.access & (Opcodes.ACC_ABSTRACT | Opcodes.ACC_NATIVE)) == 0
                && writesNoBytes(classNode.name, classes, new HashSet<>())) {
                findings.add(
                    new Finding(
                        Type.getObjectType(classNode.name).getClassName(),
                        firstLine(writeTo),
                        countInstanceFields(classNode),
                        streamConstructorStatus(classNode, classes)
                    )
                );
            }
        }
        findings.sort(Comparator.comparing(finding -> finding.className));
        return findings;
    }

    private static boolean writesNoBytes(String className, Map<String, ClassNode> classes, Set<String> visiting) {
        if (visiting.add(className) == false) {
            return false;
        }
        final ClassNode classNode = classes.get(className);
        if (classNode == null) {
            return false;
        }
        final MethodNode method = findMethod(classNode, "writeTo", WRITE_TO_DESCRIPTOR);
        if (method == null) {
            return writesNoBytes(classNode.superName, classes, visiting);
        }

        boolean callsSuper = false;
        for (AbstractInsnNode instruction : method.instructions) {
            if (instruction.getType() == AbstractInsnNode.LABEL
                || instruction.getType() == AbstractInsnNode.LINE
                || instruction.getType() == AbstractInsnNode.FRAME) {
                continue;
            }
            if (instruction.getOpcode() == Opcodes.RETURN) {
                continue;
            }
            if (instruction instanceof VarInsnNode variable
                && variable.getOpcode() == Opcodes.ALOAD
                && (variable.var == 0 || variable.var == 1)) {
                continue;
            }
            if (instruction instanceof MethodInsnNode invocation
                && invocation.getOpcode() == Opcodes.INVOKESPECIAL
                && invocation.owner.equals(classNode.superName)
                && invocation.name.equals("writeTo")
                && invocation.desc.equals(WRITE_TO_DESCRIPTOR)) {
                callsSuper = true;
                continue;
            }
            return false;
        }
        return callsSuper == false || writesNoBytes(classNode.superName, classes, visiting);
    }

    private static String streamConstructorStatus(ClassNode classNode, Map<String, ClassNode> classes) {
        final MethodNode constructor = findMethod(classNode, "<init>", STREAM_CONSTRUCTOR_DESCRIPTOR);
        if (constructor == null) {
            return "absent";
        }
        return consumesStreamInput(classNode, constructor, classes, new HashSet<>()) ? "consumes" : "empty";
    }

    private static boolean consumesStreamInput(
        ClassNode classNode,
        MethodNode constructor,
        Map<String, ClassNode> classes,
        Set<String> visiting
    ) {
        if (visiting.add(classNode.name) == false) {
            return true;
        }
        for (AbstractInsnNode instruction : constructor.instructions) {
            if (instruction instanceof MethodInsnNode invocation) {
                if (invocation.getOpcode() == Opcodes.INVOKESPECIAL
                    && invocation.owner.equals(classNode.superName)
                    && invocation.name.equals("<init>")
                    && invocation.desc.equals(STREAM_CONSTRUCTOR_DESCRIPTOR)) {
                    final ClassNode parent = classes.get(classNode.superName);
                    final MethodNode parentConstructor = parent == null
                        ? null
                        : findMethod(parent, "<init>", STREAM_CONSTRUCTOR_DESCRIPTOR);
                    if (parentConstructor == null || consumesStreamInput(parent, parentConstructor, classes, visiting)) {
                        return true;
                    }
                } else if (invocation.owner.equals(STREAM_INPUT) || invocation.desc.contains("L" + STREAM_INPUT + ";")) {
                    return true;
                }
            }
        }
        return false;
    }

    private static MethodNode findMethod(ClassNode classNode, String name, String descriptor) {
        for (MethodNode method : classNode.methods) {
            if (method.name.equals(name) && method.desc.equals(descriptor)) {
                return method;
            }
        }
        return null;
    }

    private static int firstLine(MethodNode method) {
        for (AbstractInsnNode instruction : method.instructions) {
            if (instruction instanceof LineNumberNode lineNumber) {
                return lineNumber.line;
            }
        }
        return -1;
    }

    private static int countInstanceFields(ClassNode classNode) {
        int count = 0;
        for (FieldNode field : classNode.fields) {
            if ((field.access & (Opcodes.ACC_STATIC | Opcodes.ACC_SYNTHETIC)) == 0) {
                count++;
            }
        }
        return count;
    }

    static class Finding {
        final String className;
        final int line;
        final int instanceFields;
        final String streamConstructor;

        Finding(String className, int line, int instanceFields, String streamConstructor) {
            this.className = className;
            this.line = line;
            this.instanceFields = instanceFields;
            this.streamConstructor = streamConstructor;
        }

        String classification() {
            if (instanceFields > 0 && streamConstructor.equals("empty")) {
                return "stateful-empty-reader";
            }
            if (instanceFields > 0) {
                return "stateful-no-reader";
            }
            if (streamConstructor.equals("empty")) {
                return "empty-message";
            }
            return "stateless-singleton";
        }
    }
}
