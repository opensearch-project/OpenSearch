package org.opensearch.tasks;

import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

public class ReflectionThreadAllocatedMemoryProvider implements ThreadAllocatedMemoryProvider {

    private final Object threadMXBean;
    private final Method isSupportedMethod;
    private final Method isEnabledMethod;
    private final Method getAllocatedBytesMethod;

    public ReflectionThreadAllocatedMemoryProvider(ThreadMXBean threadMXBean) {
        this.threadMXBean = threadMXBean;

        Method supported = null;
        Method enabled = null;
        Method allocated = null;

        try {
            Class<?> clazz = Class.forName("com.sun.management.ThreadMXBean");

            if (clazz.isInstance(threadMXBean)) {
                supported = clazz.getMethod("isThreadAllocatedMemorySupported");
                enabled = clazz.getMethod("isThreadAllocatedMemoryEnabled");
                allocated = clazz.getMethod("getThreadAllocatedBytes", long.class);
            }
        } catch (ClassNotFoundException | NoSuchMethodException ignored) {
        }

        this.isSupportedMethod = supported;
        this.isEnabledMethod = enabled;
        this.getAllocatedBytesMethod = allocated;
    }

    @Override
    public boolean isSupported() {
        if (isSupportedMethod == null || getAllocatedBytesMethod == null) {
            return false;
        }

        try {
            return (boolean) isSupportedMethod.invoke(threadMXBean);
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    @Override
    public boolean isEnabled() {
        if (isEnabledMethod == null) {
            return false;
        }

        try {
            return (boolean) isEnabledMethod.invoke(threadMXBean);
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    @Override
    public long getThreadAllocatedBytes(long threadId) {
        if (getAllocatedBytesMethod == null) {
            return -1;
        }

        try {
            return (long) getAllocatedBytesMethod.invoke(threadMXBean, threadId);
        } catch (ReflectiveOperationException e) {
            return -1;
        }
    }
}
