package org.opensearch.javaagent.bootstrap;

import java.security.Policy;

@SuppressWarnings("removal")
public class AgentPolicy {
    private static volatile Policy policy;

    public static void setPolicy(Policy policy) {
        AgentPolicy.policy = policy;
        System.out.println("Policy is set to: " + AgentPolicy.policy);
    }

    public static Policy getPolicy() {
        return policy;
    }
}
