/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.chaos;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;

/**
 * Java agent for bytecode injection of chaos testing
 * Usage: -javaagent:chaos-agent.jar
 */
public class ChaosAgent {

    public static void premain(String agentArgs, Instrumentation inst) {
        inst.addTransformer(new ChaosTransformer());
    }

    public static void agentmain(String agentArgs, Instrumentation inst) {
        inst.addTransformer(new ChaosTransformer(), true);
    }

    private static class ChaosTransformer implements ClassFileTransformer {

        @Override
        public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer
        ) {

            if (!shouldTransform(className)) {
                return null;
            }

            try {
                ClassPool pool = ClassPool.getDefault();
                CtClass ctClass = pool.get(className.replace('/', '.'));

                switch (className) {
                    case "org/opensearch/arrow/flight/transport/FlightTransport":
                        // transformFlightTransport(ctClass);
                        break;
                    case "org/opensearch/arrow/flight/transport/FlightTransportChannel":
                        // transformFlightTransportChannel(ctClass);
                        break;
                    case "org/opensearch/arrow/flight/transport/FlightTransportResponse":
                        // transformFlightTransportResponse(ctClass);
                        break;
                    case "org/opensearch/arrow/flight/transport/FlightServerChannel":
                        transformFlightServerChannelWithDelay(ctClass);
                        break;

                }

                return ctClass.toBytecode();
            } catch (Exception e) {
                return null;
            }
        }

        private boolean shouldTransform(String className) {
            return className.startsWith("org/opensearch/arrow/flight/transport/Flight");
        }

        private void transformFlightServerChannelWithDelay(CtClass ctClass) throws Exception {
            CtConstructor[] ctr = ctClass.getConstructors();
            ctr[0].insertBefore("org.opensearch.arrow.flight.chaos.ChaosScenario.injectChaos();");
        }
    }
}
