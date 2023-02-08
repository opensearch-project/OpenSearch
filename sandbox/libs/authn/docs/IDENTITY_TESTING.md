## Testing with Security Plugin installed

Assemble a local distribution of core:

```
./gradlew localDistro
```

Navigate to the platform specific distribution:

```
cd distribution/archives/<platform>/build/install/opensearch-<version>-SNAPSHOT
```

### Assemble the Security Plugin

Checkout the security repo: https://github.com/opensearch-project/security

Run gradlew assemble:

```
./gradlew assemble
```

### Install the Security Plugin

In the root directory of the local distribution built in core, run:

```
./bin/opensearch-plugin install --verbose file:<path-to-opensearch-security>/build/distributions/opensearch-security-<version>-SNAPSHOT.zip
```

to install the security plugin. Answer yes to the prompt. The step above may require the use of `sudo`

Navigate to the security plugin tools directory:

```
cd plugins/opensearch-security/tools
```

and run the install_demo_configuration script:

```
./install_demo_configuration.sh
```

and answer yes to all prompts.

Go back up to the root directory by running: `cd ../../..`

and open the `opensearch.yml` file:

```
vim config/opensearch.yml
```

and enter the following lines:

```
identity.enabled: true
logger.org.opensearch.identity: debug
```
