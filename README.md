# Scattersphere: Job Coordination API for Tasks

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CircleCI](https://circleci.com/gh/KenSuenobu/scattersphere.svg?style=svg)](https://circleci.com/gh/KenSuenobu/scattersphere)

Scattersphere is a lightweight job coordination API designed to run a simple 
[DAG of tasks](https://en.wikipedia.org/wiki/Directed_acyclic_graph).

It draws inspiration from many projects on Github such as Orchestra, Chronos, and
the like.  It is designed to be self-contained, and extensible.

# Prerequisites

- Java 8 SDK
- Apache Ant

## Getting Started

[There is a maintained Wiki here.](https://github.com/KenSuenobu/scattersphere/wiki)  You can also view
[sample code in the test suite here.](/scattersphere-core/src/test/scala/com/scattersphere/core/util/)

## Dependencies

- [Log4j 1.7.25](https://www.slf4j.org/download.html)
- [Logback 1.2.3](https://logback.qos.ch/download.html)
- [scala-logging 3.9.0](https://github.com/lightbend/scala-logging)
- [Scala 2.12.6](https://www.scala-lang.org)

## Contributing

Please read [CONTRIBUTING.md](/CONTRIBUTING.md)

## Building

See [BUILDING.md](/BUILDING.md)
