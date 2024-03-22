<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

<p align="center">
  <img src="docs/img/pulsar.svg" alt="Pulsar logo" width="200">
  <h1 align="center">Pulsar</h1>
</p>

<p align="center">
  Horizon component for the delivery of eventMessages with deliveryType SSE.
</p>

<p align="center">
  <a href="#prerequisites">Prerequisites</a> •
  <a href="#building-pulsar">Building Pulsar</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#running-pulsar">Running Pulsar</a>
</p>

<!--
[![REUSE status](https://api.reuse.software/badge/github.com/telekom/pubsub-horizon-pulsar)](https://api.reuse.software/info/github.com/telekom/pubsub-horizon-pulsar)
-->
[![Gradle Build and Test](https://github.com/telekom/pubsub-horizon-pulsar/actions/workflows/gradle-build.yml/badge.svg)](https://github.com/telekom/pubsub-horizon-pulsar/actions/workflows/gradle-build.yml)

## Overview
Pulsar is a Horizon component that is responsible for the SSE delivery of event messages to customers that actively call the /sse 
REST endpoint together with their subscriptionId as path parameter.

> **Note:** Pulsar is an essential part of the Horizon ecosystem. Please refer to [documentation of the entire system](https://github.com/telekom/pubsub-horizon) to get the full picture.

## Prerequisites
For the optimal setup, ensure you have:

- A running instance of Kafka
- A running instance of MongoDB
- Access to a Kubernetes cluster on which the `Subscription` (subscriber.horizon.telekom.de) custom resource definition has been registered

## Building Pulsar

### Gradle build

```bash
./gradlew build
```

### Docker build

The default docker base image is `azul/zulu-openjdk-alpine:21-jre`. This is customizable via the docker build arg `DOCKER_BASE_IMAGE`.
Please note that the default helm values configure the kafka compression type `snappy` which requires gcompat to be installed in the resulting image.
So either provide a base image with gcompat installed or change/disable the compression type in the helm values.

```bash
docker build -t horizon-pulsar:latest --build-arg="DOCKER_BASE_IMAGE=<myjvmbaseimage:1.0.0>" . 
```

#### Multi-stage Docker build

To simplify things, we have also added a mult-stage Dockerfile to the respository, which also handles the Java build of the application in a build container. The resulting image already contains "gcompat", which is necessary for Kafka compression.

```bash
docker build -t horizon-pulsar:latest . -f Dockerfile.multi-stage 
```
## Configuration
Pulsar configuration is managed through environment variables. Check the [complete list](docs/environment-variables.md) of supported environment variables for setup instructions.

## Running Pulsar
### Locally
Before you can run Pulsar locally you must have a running instance of Kafka and MongoDB locally or forwarded from a remote cluster.
Additionally, you need to have a Kubernetes config at `${user.home}/.kube/config.main` that points to the cluster you want to use.

After that you can run Pulsar in a dev mode using this command:
```shell
./gradlew bootRun
```

## Documentation

Read more about the software architecture and the general process flow of Horizon Pulsar in [docs/architecture.md](docs/architecture.md).

## Contributing

We're committed to open source, so we welcome and encourage everyone to join its developer community and contribute, whether it's through code or feedback.  
By participating in this project, you agree to abide by its [Code of Conduct](./CODE_OF_CONDUCT.md) at all times.

## Code of Conduct

This project has adopted the [Contributor Covenant](https://www.contributor-covenant.org/) in version 2.1 as our code of conduct. Please see the details in our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). All contributors must abide by the code of conduct.

By participating in this project, you agree to abide by its [Code of Conduct](./CODE_OF_CONDUCT.md) at all times.

## Licensing

This project follows the [REUSE standard for software licensing](https://reuse.software/). You can find a guide for developers at https://telekom.github.io/reuse-template/.   
Each file contains copyright and license information, and license texts can be found in the [./LICENSES](./LICENSES) folder. For more information visit https://reuse.software/.
