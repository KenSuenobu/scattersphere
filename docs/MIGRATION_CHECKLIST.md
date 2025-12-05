# Scala 2.13 Migration Checklist

## ✅ Completed Tasks

### Maven POM Updates
- [x] Updated parent pom.xml with Scala 2.13.12
- [x] Updated Spark version to 3.5.3
- [x] Updated Java version to 11
- [x] Updated ScalaTest to 3.2.18
- [x] Updated JUnit to 5.10.1 (Jupiter)
- [x] Updated maven-compiler-plugin to 3.13.0
- [x] Updated scala-maven-plugin to 4.9.2
- [x] Updated maven-shade-plugin to 3.6.0
- [x] Added scalatest-maven-plugin version
- [x] Updated all child module poms (core, tasks, base)

### Compiler Configuration
- [x] Updated scala-binary.version to 2.13
- [x] Updated scala.version to 2.13.12
- [x] Added modern compiler flags:
  - `-language:higherKinds`
  - `-Xlint:_,-byname-implicit`
  - `-Ywarn-unused:imports`
  - `-Ywarn-dead-code`
  - `-release:11`

### Code Changes

#### Scala 2.13 Compatibility
- [x] Replaced `Stream` with `LazyList` in ShellTask.scala
- [x] Replaced `Stream` with `LazyList` in DockerTask.scala

#### ScalaTest 3.2 API Updates
Test files updated (13 files total):

**Core Module:**
- [x] SimpleJobTest.scala
- [x] CancelJobTest.scala
- [x] PauseResumeJobTest.scala
- [x] ExceptionJobTest.scala
- [x] ComplicatedJobTest.scala
- [x] RealWorldTest.scala
- [x] spark/SparkCacheTest.scala

**Tasks Module:**
- [x] ShellTaskTest.scala
- [x] DelayedTaskTest.scala
- [x] DockerTaskTest.scala
- [x] RepeatingTaskTest.scala
- [x] spark/SparkTaskPiTest.scala
- [x] spark/SparkRealWorldTest.scala

All updated from:
```scala
import org.scalatest.{FlatSpec, Matchers}
class XTest extends FlatSpec with Matchers
```

To:
```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
class XTest extends AnyFlatSpec with Matchers
```

### Documentation Updates
- [x] Created SCALA_UPGRADE_2024.md with detailed upgrade notes
- [x] Updated README.md with new prerequisites and versions
- [x] Updated BUILDING.md with new build requirements

## 🔍 Verification Steps (Recommended)

### Before Deployment
- [ ] Run `mvn clean compile` to verify compilation
- [ ] Run `mvn test` to verify all tests pass
- [ ] Run integration tests if available
- [ ] Test Docker task functionality if used
- [ ] Test Shell task functionality
- [ ] Verify Spark jobs execute correctly
- [ ] Check performance benchmarks

### Environment Verification
- [ ] Verify Java 11+ is installed on target systems
- [ ] Verify Maven 3.6+ is available for builds
- [ ] Update CI/CD pipelines for Java 11
- [ ] Update Spark cluster to version 3.5.x (if using distributed mode)
- [ ] Update deployment documentation

### Dependency Verification
- [ ] Check for any transitive dependency conflicts
- [ ] Verify all Spark dependencies are Scala 2.13 compatible
- [ ] Test with production-like workloads

## 📝 Known Considerations

### Binary Compatibility
- Scala 2.13 is NOT binary compatible with Scala 2.11
- All Scala dependencies must be rebuilt for 2.13
- Mixed Scala versions in classpath will cause errors

### Spark Compatibility
- Spark 3.5.3 requires Scala 2.12 or 2.13
- Ensure Spark cluster is running compatible version
- Spark 2.x clusters must be upgraded

### Java Version
- Java 11 is minimum requirement
- Java 17 (LTS) is recommended for production
- Bytecode is compiled with `-release:11` flag

## 🚀 Deployment Strategy

### Recommended Approach
1. Deploy to development environment first
2. Run comprehensive test suite
3. Deploy to staging environment
4. Perform load testing
5. Deploy to production with rollback plan

### Rollback Plan
If issues arise:
1. Keep previous compiled artifacts
2. Revert code changes (git)
3. Redeploy with previous versions
4. Document issues encountered

## 📚 Additional Resources

- [Scala 2.13 Migration Guide](https://docs.scala-lang.org/overviews/core/collections-migration-213.html)
- [Spark 3.5.3 Release Notes](https://spark.apache.org/releases/spark-release-3-5-3.html)
- [ScalaTest 3.2 Release Notes](https://www.scalatest.org/release_notes/3.2.0)
- [JUnit 5 Migration Guide](https://junit.org/junit5/docs/current/user-guide/#migrating-from-junit4)

---
**Last Updated**: December 4, 2025

