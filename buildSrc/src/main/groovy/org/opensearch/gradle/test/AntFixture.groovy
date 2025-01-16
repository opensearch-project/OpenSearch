/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.gradle.test

import org.apache.tools.ant.taskdefs.condition.Os
import org.gradle.api.Project
import org.gradle.api.GradleException
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskProvider
import org.opensearch.gradle.AntTask
import org.opensearch.gradle.LoggedExec

import javax.inject.Inject

/**
 * A fixture for integration tests which runs in a separate process launched by Ant.
 */
class AntFixture extends AntTask implements Fixture {

    /** The path to the executable that starts the fixture. */
    @Internal
    String executable

    private final List<Object> arguments = new ArrayList<>()

    void args(Object... args) {
        arguments.addAll(args)
    }

    /**
     * Environment variables for the fixture process. The value can be any object, which
     * will have toString() called at execution time.
     */
    private final Map<String, Object> environment = new HashMap<>()

    void env(String key, Object value) {
        environment.put(key, value)
    }

    /** A flag to indicate whether the command should be executed from a shell. */
    @Internal
    boolean useShell = false

    @Internal
    int maxWaitInSeconds = 30

    /**
     * A flag to indicate whether the fixture should be run in the foreground, or spawned.
     * It is protected so subclasses can override (eg RunTask).
     */
    protected boolean spawn = true

    /**
     * A closure to call before the fixture is considered ready. The closure is passed the fixture object,
     * as well as a groovy AntBuilder, to enable running ant condition checks. The default wait
     * condition is for http on the http port.
     */
    @Internal
    Closure waitCondition = { AntFixture fixture, AntBuilder ant ->
        File tmpFile = new File(fixture.cwd, 'wait.success')
        ant.get(src: "http://${fixture.addressAndPort}",
                dest: tmpFile.toString(),
                ignoreerrors: true, // do not fail on error, so logging information can be flushed
                retries: 10)
        return tmpFile.exists()
    }

    private final TaskProvider stopTask
    private final Project project

    @Inject
    AntFixture(Project project) {
        this.project = project
        this.stopTask = createStopTask()
        finalizedBy(stopTask)
    }

    @Override
    @Internal
    TaskProvider getStopTask() {
        return stopTask
    }

    @Override
    protected void runAnt(AntBuilder ant) {
        // reset everything
        getFileSystemOperations().delete {
            it.delete(baseDir)
        }
        cwd.mkdirs()
        final String realExecutable
        final List<Object> realArgs = new ArrayList<>()
        final Map<String, Object> realEnv = environment
        // We need to choose which executable we are using. In shell mode, or when we
        // are spawning and thus using the wrapper script, the executable is the shell.
        if (useShell || spawn) {
            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                realExecutable = 'cmd'
                realArgs.add('/C')
                realArgs.add('"') // quote the entire command
            } else {
                realExecutable = 'sh'
            }
        } else {
            realExecutable = executable
            realArgs.addAll(arguments)
        }
        if (spawn) {
            writeWrapperScript(executable)
            realArgs.add(wrapperScript)
            realArgs.addAll(arguments)
        }
        if (Os.isFamily(Os.FAMILY_WINDOWS) && (useShell || spawn)) {
            realArgs.add('"')
        }
        commandString.eachLine { line -> logger.info(line) }

        ant.exec(executable: realExecutable, spawn: spawn, dir: cwd, taskname: name) {
            realEnv.each { key, value -> env(key: key, value: value) }
            realArgs.each { arg(value: it) }
        }

        String failedProp = "failed${name}"
        // first wait for resources, or the failure marker from the wrapper script
        ant.waitfor(maxwait: maxWaitInSeconds, maxwaitunit: 'second', checkevery: '500', checkeveryunit: 'millisecond', timeoutproperty: failedProp) {
            or {
                resourceexists {
                    file(file: failureMarker.toString())
                }
                and {
                    resourceexists {
                        file(file: pidFile.toString())
                    }
                    resourceexists {
                        file(file: portsFile.toString())
                    }
                }
            }
        }

        if (ant.project.getProperty(failedProp) || failureMarker.exists()) {
            fail("Failed to start ${name}")
        }

        // the process is started (has a pid) and is bound to a network interface
        // so now evaluates if the waitCondition is successful
        // TODO: change this to a loop?
        boolean success
        try {
            success = waitCondition(this, ant)
        } catch (Exception e) {
            String msg = "Wait condition caught exception for ${name}"
            logger.error(msg, e)
            fail(msg, e)
        }
        if (success == false) {
            fail("Wait condition failed for ${name}")
        }
    }

    /** Returns a debug string used to log information about how the fixture was run. */
    @Internal
    protected String getCommandString() {
        String commandString = "\n${name} configuration:\n"
        commandString += "-----------------------------------------\n"
        commandString += "  cwd: ${cwd}\n"
        commandString += "  command: ${executable} ${arguments.join(' ')}\n"
        commandString += '  environment:\n'
        environment.each { k, v -> commandString += "    ${k}: ${v}\n" }
        if (spawn) {
            commandString += "\n  [${wrapperScript.name}]\n"
            wrapperScript.eachLine('UTF-8', { line -> commandString += "    ${line}\n"})
        }
        return commandString
    }

    /**
     * Writes a script to run the real executable, so that stdout/stderr can be captured.
     * TODO: this could be removed if we do use our own ProcessBuilder and pump output from the process
     */
    private void writeWrapperScript(String executable) {
        wrapperScript.parentFile.mkdirs()
        String argsPasser = '"$@"'
        String exitMarker = "; if [ \$? != 0 ]; then touch run.failed; fi"
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            argsPasser = '%*'
            exitMarker = "\r\n if \"%errorlevel%\" neq \"0\" ( type nul >> run.failed )"
        }
        wrapperScript.setText("\"${executable}\" ${argsPasser} > run.log 2>&1 ${exitMarker}", 'UTF-8')
    }

    /** Fail the build with the given message, and logging relevant info*/
    private void fail(String msg, Exception... suppressed) {
        if (logger.isInfoEnabled() == false) {
            // We already log the command at info level. No need to do it twice.
            commandString.eachLine { line -> logger.error(line) }
        }
        logger.error("${name} output:")
        logger.error("-----------------------------------------")
        logger.error("  failure marker exists: ${failureMarker.exists()}")
        logger.error("  pid file exists: ${pidFile.exists()}")
        logger.error("  ports file exists: ${portsFile.exists()}")
        // also dump the log file for the startup script (which will include ES logging output to stdout)
        if (runLog.exists()) {
            logger.error("\n  [log]")
            runLog.eachLine { line -> logger.error("    ${line}") }
        }
        logger.error("-----------------------------------------")
        GradleException toThrow = new GradleException(msg)
        for (Exception e : suppressed) {
            toThrow.addSuppressed(e)
        }
        throw toThrow
    }

    /** Adds a task to kill an opensearch node with the given pidfile */
    private TaskProvider createStopTask() {
        final AntFixture fixture = this
        final Object pid = "${ -> fixture.pid }"
        TaskProvider<Exec> stop = project.tasks.register("${name}#stop", LoggedExec)
        stop.configure {
            onlyIf { fixture.pidFile.exists() }
            doFirst {
                logger.info("Shutting down ${fixture.name} with pid ${pid}")
            }

            if (Os.isFamily(Os.FAMILY_WINDOWS)) {
                executable = 'Taskkill'
                args('/PID', pid, '/F')
            } else {
                executable = 'kill'
                args('-9', pid)
            }
            doLast {
                getFileSystemOperations().delete {
                    it.delete(fixture.pidFile)
                }
            }

        }

        return stop
    }

    /**
     * A path relative to the build dir that all configuration and runtime files
     * will live in for this fixture
     */
    @Internal
    protected File getBaseDir() {
        return new File(project.buildDir, "fixtures/${name}")
    }

    /** Returns the working directory for the process. Defaults to "cwd" inside baseDir. */
    @Internal
    protected File getCwd() {
        return new File(baseDir, 'cwd')
    }

    /** Returns the file the process writes its pid to. Defaults to "pid" inside baseDir. */
    @Internal
    protected File getPidFile() {
        return new File(baseDir, 'pid')
    }

    /** Reads the pid file and returns the process' pid */
    @Internal
    int getPid() {
        return Integer.parseInt(pidFile.getText('UTF-8').trim())
    }

    /** Returns the file the process writes its bound ports to. Defaults to "ports" inside baseDir. */
    @Internal
    protected File getPortsFile() {
        return new File(baseDir, 'ports')
    }

    /** Returns an address and port suitable for a uri to connect to this node over http */
    @Internal
    String getAddressAndPort() {
        return portsFile.readLines("UTF-8").get(0)
    }

    /** Returns a file that wraps around the actual command when {@code spawn == true}. */
    @Internal
    protected File getWrapperScript() {
        return new File(cwd, Os.isFamily(Os.FAMILY_WINDOWS) ? 'run.bat' : 'run')
    }

    /** Returns a file that the wrapper script writes when the command failed. */
    @Internal
    protected File getFailureMarker() {
        return new File(cwd, 'run.failed')
    }

    /** Returns a file that the wrapper script writes when the command failed. */
    @Internal
    protected File getRunLog() {
        return new File(cwd, 'run.log')
    }
}
