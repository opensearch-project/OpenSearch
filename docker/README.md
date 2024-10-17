# Docker environments

Multipurpose Docker environments to run, test and build `wazuh-indexer`.

## Pre-requisites

1. Install [Docker][docker] as per its instructions.

1. Your workstation must meet the minimum hardware requirements:

   - 8 GB of RAM (minimum)
   - 4 cores

   The more resources the better ☺

1. Clone the [wazuh-indexer][wi-repo].

## Development environments

Use the `dev/dev.sh` script to start a development environment.

Example:

```bash
Usage: ./dev.sh {up|down|stop}
```

Once the `wi-dev:x.y.z` container is up, attach a shell to it and run `./gradlew run` to start the application.

## Containers to generate packages

Use the `ci/ci.sh` script to start provisioned containers to generate packages.

```bash
Usage: ./ci.sh {up|down|stop}
```

Refer to [packaging_scripts/README.md](../packaging_scripts/README.md) for details about how to build packages.

[docker]: https://docs.docker.com/engine/install
[wi-repo]: https://github.com/wazuh/wazuh-indexer

## Building Docker images

The [prod](./prod) folder contains the code to build Docker images. A tarball of `wazuh-indexer` needs to be located at the same level that the Dockerfile. Below there is an example of the command needed to build the image. Set the build arguments and the image tag accordingly.

```console
docker build --build-arg="VERSION=4.10.2" --build-arg="INDEXER_TAR_NAME=wazuh-indexer-4.10.2-1_linux-x64_cfca84f.tar.gz" --tag=wazuh-indexer:4.10.2 --progress=plain --no-cache .
```

Then, start a container with:

```console
docker run -it --rm wazuh-indexer:4.10.2 
```
