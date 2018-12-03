# Scattersphere: Job Coordination API for Tasks

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/scattersphere)
[![CircleCI](https://circleci.com/gh/KenSuenobu/scattersphere.svg?style=svg)](https://circleci.com/gh/KenSuenobu/scattersphere)

Scattersphere is a lightweight job coordination API designed to run a simple 
[DAG of tasks](https://en.wikipedia.org/wiki/Directed_acyclic_graph).

It draws inspiration from many projects on Github such as Orchestra, Chronos, and
the like.  It is designed to be self-contained, and extensible.

## Prerequisites

- Java 8 SDK
- Apache Ant

## Getting Started

[There is a maintained Wiki here.](https://github.com/KenSuenobu/scattersphere/wiki)  You can also view
[sample code in the test suite here.](/scattersphere-core/src/test/scala/io/buildfactory/scattersphere/core/util)

## Dependencies

- [Scala 2.11.12](https://www.scala-lang.org)
- [Spark 2.3.2](http://spark.apache.org)

**And that's all**

NOTE: Spark is only required if you plan on coordinating jobs using the Spark framework.  If you don't,
you can just use Scattersphere in a single server setup.  Scattersphere will be run as a server on
distributed servers eventually.

## Contributing

Please read [CONTRIBUTING.md](/scattersphere-docs/CONTRIBUTING.md)

## Building

See [BUILDING.md](/scattersphere-docs/BUILDING.md)
