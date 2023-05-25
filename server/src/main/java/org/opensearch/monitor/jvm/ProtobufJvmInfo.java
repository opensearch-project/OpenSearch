/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.monitor.jvm;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.lucene.util.Constants;
import org.opensearch.common.Booleans;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.node.ProtobufReportingService;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ManagementPermission;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Holds information about the JVM
*
* @opensearch.internal
*/
public class ProtobufJvmInfo implements ProtobufReportingService.ProtobufInfo {

    private static ProtobufJvmInfo INSTANCE;

    static {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        long heapInit = memoryMXBean.getHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getInit();
        long heapMax = memoryMXBean.getHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getHeapMemoryUsage().getMax();
        long nonHeapInit = memoryMXBean.getNonHeapMemoryUsage().getInit() < 0 ? 0 : memoryMXBean.getNonHeapMemoryUsage().getInit();
        long nonHeapMax = memoryMXBean.getNonHeapMemoryUsage().getMax() < 0 ? 0 : memoryMXBean.getNonHeapMemoryUsage().getMax();
        long directMemoryMax = 0;
        try {
            Class<?> vmClass = Class.forName("sun.misc.VM");
            directMemoryMax = (Long) vmClass.getMethod("maxDirectMemory").invoke(null);
        } catch (Exception t) {
            // ignore
        }
        String[] inputArguments = runtimeMXBean.getInputArguments().toArray(new String[0]);
        Mem mem = new Mem(heapInit, heapMax, nonHeapInit, nonHeapMax, directMemoryMax);

        String bootClassPath;
        try {
            bootClassPath = runtimeMXBean.getBootClassPath();
        } catch (UnsupportedOperationException e) {
            // oracle java 9
            bootClassPath = System.getProperty("sun.boot.class.path");
            if (bootClassPath == null) {
                // something else
                bootClassPath = "<unknown>";
            }
        }
        String classPath = runtimeMXBean.getClassPath();
        Map<String, String> systemProperties = Collections.unmodifiableMap(runtimeMXBean.getSystemProperties());

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        String[] gcCollectors = new String[gcMxBeans.size()];
        for (int i = 0; i < gcMxBeans.size(); i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            gcCollectors[i] = gcMxBean.getName();
        }

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        String[] memoryPools = new String[memoryPoolMXBeans.size()];
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
            memoryPools[i] = memoryPoolMXBean.getName();
        }

        String onError = null;
        String onOutOfMemoryError = null;
        String useCompressedOops = "unknown";
        String useG1GC = "unknown";
        long g1RegisionSize = -1;
        String useSerialGC = "unknown";
        long configuredInitialHeapSize = -1;
        long configuredMaxHeapSize = -1;
        try {
            @SuppressWarnings("unchecked")
            Class<? extends PlatformManagedObject> clazz = (Class<? extends PlatformManagedObject>) Class.forName(
                "com.sun.management.HotSpotDiagnosticMXBean"
            );
            Class<?> vmOptionClazz = Class.forName("com.sun.management.VMOption");
            PlatformManagedObject hotSpotDiagnosticMXBean = ManagementFactory.getPlatformMXBean(clazz);
            Method vmOptionMethod = clazz.getMethod("getVMOption", String.class);
            Method valueMethod = vmOptionClazz.getMethod("getValue");

            try {
                Object onErrorObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "OnError");
                onError = (String) valueMethod.invoke(onErrorObject);
            } catch (Exception ignored) {}

            try {
                Object onOutOfMemoryErrorObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "OnOutOfMemoryError");
                onOutOfMemoryError = (String) valueMethod.invoke(onOutOfMemoryErrorObject);
            } catch (Exception ignored) {}

            try {
                Object useCompressedOopsVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseCompressedOops");
                useCompressedOops = (String) valueMethod.invoke(useCompressedOopsVmOptionObject);
            } catch (Exception ignored) {}

            try {
                Object useG1GCVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseG1GC");
                useG1GC = (String) valueMethod.invoke(useG1GCVmOptionObject);
                Object regionSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "G1HeapRegionSize");
                g1RegisionSize = Long.parseLong((String) valueMethod.invoke(regionSizeVmOptionObject));
            } catch (Exception ignored) {}

            try {
                Object initialHeapSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "InitialHeapSize");
                configuredInitialHeapSize = Long.parseLong((String) valueMethod.invoke(initialHeapSizeVmOptionObject));
            } catch (Exception ignored) {}

            try {
                Object maxHeapSizeVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "MaxHeapSize");
                configuredMaxHeapSize = Long.parseLong((String) valueMethod.invoke(maxHeapSizeVmOptionObject));
            } catch (Exception ignored) {}

            try {
                Object useSerialGCVmOptionObject = vmOptionMethod.invoke(hotSpotDiagnosticMXBean, "UseSerialGC");
                useSerialGC = (String) valueMethod.invoke(useSerialGCVmOptionObject);
            } catch (Exception ignored) {}

        } catch (Exception ignored) {

        }

        final boolean bundledJdk = Booleans.parseBoolean(System.getProperty("opensearch.bundled_jdk", Boolean.FALSE.toString()));
        final Boolean usingBundledJdk = bundledJdk ? usingBundledJdk() : null;

        INSTANCE = new ProtobufJvmInfo(
            JvmPid.getPid(),
            System.getProperty("java.version"),
            runtimeMXBean.getVmName(),
            runtimeMXBean.getVmVersion(),
            runtimeMXBean.getVmVendor(),
            bundledJdk,
            usingBundledJdk,
            runtimeMXBean.getStartTime(),
            configuredInitialHeapSize,
            configuredMaxHeapSize,
            mem,
            inputArguments,
            bootClassPath,
            classPath,
            systemProperties,
            gcCollectors,
            memoryPools,
            onError,
            onOutOfMemoryError,
            useCompressedOops,
            useG1GC,
            useSerialGC,
            g1RegisionSize
        );
    }

    @SuppressForbidden(reason = "PathUtils#get")
    private static boolean usingBundledJdk() {
        /*
        * We are using the bundled JDK if java.home is the jdk sub-directory of our working directory. This is because we always set
        * the working directory of Elasticsearch to home, and the bundled JDK is in the jdk sub-directory there.
        */
        final String javaHome = System.getProperty("java.home");
        final String userDir = System.getProperty("user.dir");
        if (Constants.MAC_OS_X) {
            return PathUtils.get(javaHome).equals(PathUtils.get(userDir).resolve("jdk.app/Contents/Home").toAbsolutePath());
        } else {
            return PathUtils.get(javaHome).equals(PathUtils.get(userDir).resolve("jdk").toAbsolutePath());
        }
    }

    public static ProtobufJvmInfo jvmInfo() {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new ManagementPermission("monitor"));
            sm.checkPropertyAccess("*");
        }
        return INSTANCE;
    }

    private final long pid;
    private final String version;
    private final String vmName;
    private final String vmVersion;
    private final String vmVendor;
    private final boolean bundledJdk;
    private final Boolean usingBundledJdk;
    private final long startTime;
    private final long configuredInitialHeapSize;
    private final long configuredMaxHeapSize;
    private final Mem mem;
    private final String[] inputArguments;
    private final String bootClassPath;
    private final String classPath;
    private final Map<String, String> systemProperties;
    private final String[] gcCollectors;
    private final String[] memoryPools;
    private final String onError;
    private final String onOutOfMemoryError;
    private final String useCompressedOops;
    private final String useG1GC;
    private final String useSerialGC;
    private final long g1RegionSize;

    private ProtobufJvmInfo(
        long pid,
        String version,
        String vmName,
        String vmVersion,
        String vmVendor,
        boolean bundledJdk,
        Boolean usingBundledJdk,
        long startTime,
        long configuredInitialHeapSize,
        long configuredMaxHeapSize,
        Mem mem,
        String[] inputArguments,
        String bootClassPath,
        String classPath,
        Map<String, String> systemProperties,
        String[] gcCollectors,
        String[] memoryPools,
        String onError,
        String onOutOfMemoryError,
        String useCompressedOops,
        String useG1GC,
        String useSerialGC,
        long g1RegionSize
    ) {
        this.pid = pid;
        this.version = version;
        this.vmName = vmName;
        this.vmVersion = vmVersion;
        this.vmVendor = vmVendor;
        this.bundledJdk = bundledJdk;
        this.usingBundledJdk = usingBundledJdk;
        this.startTime = startTime;
        this.configuredInitialHeapSize = configuredInitialHeapSize;
        this.configuredMaxHeapSize = configuredMaxHeapSize;
        this.mem = mem;
        this.inputArguments = inputArguments;
        this.bootClassPath = bootClassPath;
        this.classPath = classPath;
        this.systemProperties = systemProperties;
        this.gcCollectors = gcCollectors;
        this.memoryPools = memoryPools;
        this.onError = onError;
        this.onOutOfMemoryError = onOutOfMemoryError;
        this.useCompressedOops = useCompressedOops;
        this.useG1GC = useG1GC;
        this.useSerialGC = useSerialGC;
        this.g1RegionSize = g1RegionSize;
    }

    public ProtobufJvmInfo(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        pid = in.readInt64();
        version = in.readString();
        vmName = in.readString();
        vmVersion = in.readString();
        vmVendor = in.readString();
        bundledJdk = in.readBool();
        usingBundledJdk = protobufStreamInput.readOptionalBoolean();
        startTime = in.readInt64();
        inputArguments = new String[in.readInt32()];
        for (int i = 0; i < inputArguments.length; i++) {
            inputArguments[i] = in.readString();
        }
        bootClassPath = in.readString();
        classPath = in.readString();
        systemProperties = protobufStreamInput.readMap(CodedInputStream::readString, CodedInputStream::readString);
        mem = new Mem(in);
        gcCollectors = protobufStreamInput.readStringArray();
        memoryPools = protobufStreamInput.readStringArray();
        useCompressedOops = in.readString();
        // the following members are only used locally for bootstrap checks, never serialized nor printed out
        this.configuredMaxHeapSize = -1;
        this.configuredInitialHeapSize = -1;
        this.onError = null;
        this.onOutOfMemoryError = null;
        this.useG1GC = "unknown";
        this.useSerialGC = "unknown";
        this.g1RegionSize = -1;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt64NoTag(pid);
        out.writeStringNoTag(version);
        out.writeStringNoTag(vmName);
        out.writeStringNoTag(vmVersion);
        out.writeStringNoTag(vmVendor);
        out.writeBoolNoTag(bundledJdk);
        protobufStreamOutput.writeOptionalBoolean(usingBundledJdk);
        out.writeInt64NoTag(startTime);
        out.writeInt32NoTag(inputArguments.length);
        for (String inputArgument : inputArguments) {
            out.writeStringNoTag(inputArgument);
        }
        out.writeStringNoTag(bootClassPath);
        out.writeStringNoTag(classPath);
        out.writeInt32NoTag(this.systemProperties.size());
        for (Map.Entry<String, String> entry : systemProperties.entrySet()) {
            out.writeStringNoTag(entry.getKey());
            out.writeStringNoTag(entry.getValue());
        }
        mem.writeTo(out);
        protobufStreamOutput.writeStringArray(gcCollectors);
        protobufStreamOutput.writeStringArray(memoryPools);
        out.writeStringNoTag(useCompressedOops);
    }

    /**
     * The process id.
    */
    public long pid() {
        return this.pid;
    }

    /**
     * The process id.
    */
    public long getPid() {
        return pid;
    }

    public String version() {
        return this.version;
    }

    public String getVersion() {
        return this.version;
    }

    public int versionAsInteger() {
        try {
            int i = 0;
            StringBuilder sVersion = new StringBuilder();
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion.append(version.charAt(i));
                }
            }
            if (i == 0) {
                return -1;
            }
            return Integer.parseInt(sVersion.toString());
        } catch (Exception e) {
            return -1;
        }
    }

    public int versionUpdatePack() {
        try {
            int i = 0;
            StringBuilder sVersion = new StringBuilder();
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
                if (version.charAt(i) != '.') {
                    sVersion.append(version.charAt(i));
                }
            }
            if (i == 0) {
                return -1;
            }
            Integer.parseInt(sVersion.toString());
            int from;
            if (version.charAt(i) == '_') {
                // 1.7.0_4
                from = ++i;
            } else if (version.charAt(i) == '-' && version.charAt(i + 1) == 'u') {
                // 1.7.0-u2-b21
                i = i + 2;
                from = i;
            } else {
                return -1;
            }
            for (; i < version.length(); i++) {
                if (!Character.isDigit(version.charAt(i)) && version.charAt(i) != '.') {
                    break;
                }
            }
            if (from == i) {
                return -1;
            }
            return Integer.parseInt(version.substring(from, i));
        } catch (Exception e) {
            return -1;
        }
    }

    public String getVmName() {
        return this.vmName;
    }

    public String getVmVersion() {
        return this.vmVersion;
    }

    public String getVmVendor() {
        return this.vmVendor;
    }

    public boolean getBundledJdk() {
        return bundledJdk;
    }

    public Boolean getUsingBundledJdk() {
        return usingBundledJdk;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public Mem getMem() {
        return this.mem;
    }

    public String[] getInputArguments() {
        return this.inputArguments;
    }

    public String getBootClassPath() {
        return this.bootClassPath;
    }

    public String getClassPath() {
        return this.classPath;
    }

    public Map<String, String> getSystemProperties() {
        return this.systemProperties;
    }

    public long getConfiguredInitialHeapSize() {
        return configuredInitialHeapSize;
    }

    public long getConfiguredMaxHeapSize() {
        return configuredMaxHeapSize;
    }

    public String onError() {
        return onError;
    }

    public String onOutOfMemoryError() {
        return onOutOfMemoryError;
    }

    /**
     * The value of the JVM flag UseCompressedOops, if available otherwise
    * "unknown". The value "unknown" indicates that an attempt was
    * made to obtain the value of the flag on this JVM and the attempt
    * failed.
    *
    * @return the value of the JVM flag UseCompressedOops or "unknown"
    */
    public String useCompressedOops() {
        return this.useCompressedOops;
    }

    public String useG1GC() {
        return this.useG1GC;
    }

    public String useSerialGC() {
        return this.useSerialGC;
    }

    public long getG1RegionSize() {
        return g1RegionSize;
    }

    public String[] getGcCollectors() {
        return gcCollectors;
    }

    public String[] getMemoryPools() {
        return memoryPools;
    }

    /**
     * Memory information.
    *
    * @opensearch.internal
    */
    public static class Mem implements ProtobufWriteable {

        private final long heapInit;
        private final long heapMax;
        private final long nonHeapInit;
        private final long nonHeapMax;
        private final long directMemoryMax;

        public Mem(long heapInit, long heapMax, long nonHeapInit, long nonHeapMax, long directMemoryMax) {
            this.heapInit = heapInit;
            this.heapMax = heapMax;
            this.nonHeapInit = nonHeapInit;
            this.nonHeapMax = nonHeapMax;
            this.directMemoryMax = directMemoryMax;
        }

        public Mem(CodedInputStream in) throws IOException {
            this.heapInit = in.readInt64();
            this.heapMax = in.readInt64();
            this.nonHeapInit = in.readInt64();
            this.nonHeapMax = in.readInt64();
            this.directMemoryMax = in.readInt64();
        }

        @Override
        public void writeTo(CodedOutputStream out) throws IOException {
            out.writeInt64NoTag(heapInit);
            out.writeInt64NoTag(heapMax);
            out.writeInt64NoTag(nonHeapInit);
            out.writeInt64NoTag(nonHeapMax);
            out.writeInt64NoTag(directMemoryMax);
        }

        public ByteSizeValue getHeapInit() {
            return new ByteSizeValue(heapInit);
        }

        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        public ByteSizeValue getNonHeapInit() {
            return new ByteSizeValue(nonHeapInit);
        }

        public ByteSizeValue getNonHeapMax() {
            return new ByteSizeValue(nonHeapMax);
        }

        public ByteSizeValue getDirectMemoryMax() {
            return new ByteSizeValue(directMemoryMax);
        }
    }
}
