package org.opensearch.tasks;

public interface ThreadAllocatedMemoryProvider {
    boolean isSupported();

    boolean isEnabled();

    long getThreadAllocatedBytes(long threadId);
}
