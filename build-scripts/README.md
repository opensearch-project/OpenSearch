# Wazuh Indexer packages generation guide

This guide includes instructions to generate distribution packages locally using Docker.

Wazuh Indexer supports any of these combinations:

- distributions: `['tar', 'deb', 'rpm']`
- architectures: `['x64', 'arm64']`

Windows is currently not supported.

The process to build packages requires Docker and Docker Compose.

- [Install Docker](https://docs.docker.com/engine/install/)
- [Install Docker Compose](https://docs.docker.com/compose/install/linux/)

Before you get started, make sure to clean your environment by running `./gradlew clean`.

## Pre-requisites

1. Install [Docker](https://docs.docker.com/engine/install/) as per its instructions.

2. Your workstation must meet the minimum hardware requirements:

   - 8 GB of RAM (minimum)
   - 4 cores

   The more resources the better ☺

3. Clone the [wazuh-indexer](https://github.com/wazuh/wazuh-indexer).

## Building wazuh-indexer packages

The `builder` image automates the build and assemble process for the Wazuh Indexer and its plugins, making it easy to create packages on any system.

Use the script under `wazuh-indexer/build-scripts/builder/builder.sh` to build a package.

```bash
./builder.sh -h
Usage: ./builder.sh [args]

Arguments:
-p INDEXER_PLUGINS_BRANCH     [Optional] wazuh-indexer-plugins repo branch, default is 'main'.
-R REVISION                   [Optional] Package revision, default is '0'.
-s STAGE                      [Optional] Staging build, default is 'false'.
-d DISTRIBUTION               [Optional] Distribution, default is 'rpm'.
-a ARCHITECTURE               [Optional] Architecture, default is 'x64'.
-D      Destroy the docker environment
-h      Print help
```

The example below it will generate a wazuh-indexer package for Debian based systems, for the x64 architecture, using 1 as revision number and using the production naming convention.

```bash
# Wihtin wazuh-indexer/build-scripts/builder
bash builder.sh -d deb -a x64 -R 1 -s true
```

The resulting package will be stored at `wazuh-indexer/artifacts/dist`.

> The `STAGE` option defines the naming of the package. When set to `false`, the package will be unequivocally named with the commits' SHA of the `wazuh-indexer` and `wazuh-indexer-plugins`repositories, in that order. For example: `wazuh-indexer_<version>-<revision>_x86_64_aff30960363-846f143.rpm`.

## Building wazuh-indexer Docker images

The [docker](./docker) folder contains the code to build Docker images. Below there is an example of the command needed to build the image. Set the build arguments and the image tag accordingly.

The Docker image is built from a wazuh-indexer tarball (tar.gz), which must be present in the same folder as the Dockerfile in `wazuh-indexer/build-scripts/docker`.

```bash
docker build --build-arg="VERSION=<version>" --build-arg="INDEXER_TAR_NAME=wazuh-indexer_<version>-<revision>_linux-x64.tar.gz" --tag=wazuh-indexer:<version>-<revision> --progress=plain --no-cache .
```

Then, start a container with:

```bash
docker run -p 9200:9200 -it --rm wazuh-indexer:<version>-<revision>
```

The `build-and-push-docker-image.sh` script automates the process to build and push Wazuh Indexer Docker images to our repository in quay.io. The script takes serveral parameters. Use the `-h` option to display them.

To push images, credentials must be set at environment level:

- QUAY_USERNAME
- QUAY_TOKEN

```bash
Usage: build-scripts/build-and-push-docker-image.sh [args]

Arguments:
-n NAME         [required] Tarball name.
-r REVISION     [Optional] Revision qualifier, default is 0.
-h help
```

The script will stop if the credentials are not set, or if any of the required parameters are not provided.

This script is used in the `build-push-docker-image.yml` **GitHub Workflow**, which is used to automate the process even more. When possible, **prefer this method**.
