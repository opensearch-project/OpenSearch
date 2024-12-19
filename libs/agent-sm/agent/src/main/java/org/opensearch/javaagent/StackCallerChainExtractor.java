package org.opensearch.javaagent;

import java.lang.StackWalker.StackFrame;
import java.security.ProtectionDomain;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StackCallerChainExtractor implements Function<Stream<StackFrame>, List<ProtectionDomain>> {
    @Override
    public List<ProtectionDomain> apply(Stream<StackFrame> frames) {
        return frames.map(StackFrame::getDeclaringClass).map(Class::getProtectionDomain).distinct().collect(Collectors.toList());
    }
}
