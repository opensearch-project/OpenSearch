/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.espresso.sandbox;

import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.IOAccess;

/**
 * GraalVM Sandbox
 */
public class Sandbox {
    /**
     * GraalVM Sandbox runner
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final String opensearchHome = args[0];
        System.out.println("OpenSearch home: " + opensearchHome);

        final Engine engine = Engine.newBuilder().build();

        System.out.println("Host JVM version: " + Runtime.version());
        final String plugin = loadPlugin(opensearchHome, engine);
        System.out.println(plugin);
    }

    private static String loadPlugin(String opensearchHome, Engine engine) throws IOException {
        // See please:
        // - https://github.com/oracle/graal/issues/10239
        // -
        // https://github.com/oracle/graal/blob/master/espresso/src/com.oracle.truffle.espresso/src/com/oracle/truffle/espresso/EspressoOptions.java
        // -
        // https://github.com/oracle/graal/blob/master/espresso/src/com.oracle.truffle.espresso.launcher/src/com/oracle/truffle/espresso/launcher/EspressoLauncher.java
        final Context context = Context.newBuilder("java")
            .option("java.JavaHome", "/usr/lib/jvm/java-21-openjdk-amd64/")
            .option(
                "java.Classpath",
                ("${opensearchHome}/lib/lucene-core-9.12.0.jar:"
                    + "${opensearchHome}/lib/opensearch-cli-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/lib/jackson-core-2.17.2.jar:"
                    + "${opensearchHome}/lib/jackson-dataformat-cbor-2.17.2.jar:"
                    + "${opensearchHome}/lib/jackson-dataformat-smile-2.17.2.jar:"
                    + "${opensearchHome}/lib/jackson-dataformat-yaml-2.17.2.jar:"
                    + "${opensearchHome}/lib/snakeyaml-2.1.jar:"
                    + "${opensearchHome}/lib/opensearch-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/lib/opensearch-core-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/lib/opensearch-common-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/lib/opensearch-x-content-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/lib/opensearch-secure-sm-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/lib/log4j-core-2.21.0.jar:"
                    + "${opensearchHome}/lib/log4j-jul-2.21.0.jar:"
                    + "${opensearchHome}/lib/log4j-api-2.21.0.jar:"
                    + "${opensearchHome}/plugins/identity-shiro/slf4j-api-1.7.36.jar:"
                    + "${opensearchHome}/plugins/identity-shiro/passay-1.6.3.jar:"
                    + "${opensearchHome}/plugins/identity-shiro/identity-shiro-3.0.0-SNAPSHOT.jar:"
                    + "${opensearchHome}/plugins/identity-shiro/shiro-core-1.13.0.jar").replaceAll(
                        "[$][{]opensearchHome[}]",
                        opensearchHome
                    )
            )
            .option("java.Properties.java.security.manager", "allow")
            .option("java.PolyglotInterfaceMappings", getInterfaceMappings())
            .option("java.Polyglot", "true")
            .allowExperimentalOptions(true)
            .allowNativeAccess(true)
            .allowCreateThread(true)
            .allowHostAccess(HostAccess.NONE)
            .allowIO(IOAccess.NONE)
            .allowPolyglotAccess(PolyglotAccess.newBuilder().allowBindingsAccess("java").build())
            .engine(engine)
            .build();

        final Value runtime = context.getBindings("java").getMember("java.lang.Runtime");
        System.out.println("Polyglot JVM version: " + runtime.invokeMember("version").toString());

        final Path homePath = new File(opensearchHome).toPath();
        final Path configPath = new File(opensearchHome + "/config").toPath();
        context.getBindings("java")
            .getMember("org.opensearch.bootstrap.QuickBoostrap")
            .invokeMember("bootstrap", configPath.toString(), homePath.toString());

        final Value securityManager = context.getBindings("java").getMember("java.lang.System").invokeMember("getSecurityManager");
        System.out.println("Security Manager? " + securityManager.toString());

        final Value settings = context.getBindings("java").getMember("org.opensearch.common.settings.Settings").getMember("EMPTY");
        final Value result = context.getBindings("java").getMember("org.opensearch.identity.shiro.ShiroIdentityPlugin");
        final Value instance = result.newInstance(settings);
        System.out.println("Shiro Plugin? " + instance.toString());

        final Client client = new NodeClient(Settings.EMPTY, new ThreadPool(Settings.EMPTY));
        final Value socket = instance.invokeMember("getSocket", client);
        return socket.toString();
    }

    private static String getInterfaceMappings() {
        return "org.opensearch.client.Client;" + "org.opensearch.client.AdminClient;";
    }
}
