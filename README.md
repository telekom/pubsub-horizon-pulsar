<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

<img src="docs/img/pulsar.svg" alt="pulsar-logo" height="250px"/>
<h1>Pulsar</h1>

<p align="center">
  Horizon component for the delivery of eventMessages with deliveryType SSE.
</p>

<p align="center">
  <a href="#prerequisites">Prerequisites</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#running-pulsar">Running Pulsar</a>
</p>

## Overview
Horizon component that is responsible for the SSE delivery of event messages to customers that actively call the /sse 
REST endpoint together with their subscriptionId as path parameter.

### Prerequisites
To test changes locally, ensure the following prerequisites are met:

- Have a Kubernetes config at `${user.home}/.kube/config.laptop-awsd-live-system` pointing to a non-production cluster.
- Run Kafka on your local machine 
- Run MongoDB on your local machine
- Having a namespace as configured in `kubernetes.informer.namespace` and a CustomResource `subscriptions.subscriber.horizon.telekom.de`.
- The resource definition can be found in the [Horizon Essentials Helm Chart](https://gitlab.devops.telekom.de/dhei/teams/pandora/argocd-charts/horizon-3.0/essentials/-/tree/main?ref_type=heads)

## Configuration
Pulsar configuration is managed through environment variables.

## Running Pulsar
Follow these steps to set up Horizon Pulsar for local development. Check the [complete list](docs/environment-variables.md) of supported environment variables for setup instruction

### 1. Clone the Repository

```bash
git clone [repository-url]
cd pulsar
```

#### 2. Install Dependencies
```bash
./gradlew build
```

#### 3. Start docker-compose
```bash
docker-compuse up -d
```

#### 4. Run Locally
```bash
./gradlew bootRun --args='--spring.profiles.active=dev'
```
This command will start Horizon Pulsar in development mode.

## Code of Conduct

This project has adopted the [Contributor Covenant](https://www.contributor-covenant.org/) in version 2.1 as our code of conduct. Please see the details in our [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). All contributors must abide by the code of conduct.

By participating in this project, you agree to abide by its [Code of Conduct](./CODE_OF_CONDUCT.md) at all times.

## Licensing

This project follows the [REUSE standard for software licensing](https://reuse.software/).
Each file contains copyright and license information, and license texts can be found in the [./LICENSES](./LICENSES) folder. For more information visit https://reuse.software/.

### REUSE

For a comprehensive guide on how to use REUSE for licensing in this repository, visit https://telekom.github.io/reuse-template/.   
A brief summary follows below:

The [reuse tool](https://github.com/fsfe/reuse-tool) can be used to verify and establish compliance when new files are added.

For more information on the reuse tool visit https://github.com/fsfe/reuse-tool.