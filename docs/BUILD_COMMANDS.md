# Build Commands Reference

Quick reference for building and testing the upgraded Scattersphere project.

## Prerequisites Check

Before building, verify your environment:

```bash
# Check Java version (should be 11 or higher)
java -version

# Check Maven version (should be 3.6 or higher)
mvn -version

# Check Scala version being used by Maven (after first build)
mvn dependency:tree | grep scala-library
```

## Build Commands

### Full Clean Build
```bash
cd /Users/kenji/Development/scattersphere
mvn clean install
```

### Build Without Tests (Fast)
```bash
mvn clean install -DskipTests
```

### Build Without Tests and Without Documentation
```bash
mvn clean install -DskipTests -Dmaven.javadoc.skip=true
```

### Compile Only (No Package)
```bash
mvn clean compile
```

## Test Commands

### Run All Tests
```bash
mvn test
```

### Run Tests for Specific Module
```bash
# Core module only
mvn test -pl scattersphere-core

# Tasks module only
mvn test -pl scattersphere-tasks

# Base module only
mvn test -pl scattersphere-base
```

### Run Specific Test Class
```bash
mvn test -Dtest=SimpleJobTest
mvn test -Dtest=ShellTaskTest
```

### Run Tests with Verbose Output
```bash
mvn test -X
```

### Run Tests and Generate Reports
```bash
mvn clean test surefire-report:report
# Reports will be in target/surefire-reports/
```

## Dependency Commands

### Display Dependency Tree
```bash
mvn dependency:tree
```

### Check for Dependency Updates
```bash
mvn versions:display-dependency-updates
```

### Check for Plugin Updates
```bash
mvn versions:display-plugin-updates
```

### Resolve and Download Dependencies
```bash
mvn dependency:resolve
```

## Clean Commands

### Clean Build Directory
```bash
mvn clean
```

### Clean Maven Cache for This Project
```bash
rm -rf ~/.m2/repository/io/buildfactory/scattersphere*
```

### Full Clean (Including IntelliJ/IDE Files)
```bash
mvn clean
find . -name "*.iml" -delete
find . -name ".idea" -type d -exec rm -rf {} +
find . -name "target" -type d -exec rm -rf {} +
```

## Package Commands

### Create JAR Files
```bash
mvn package
```

### Create JAR with Dependencies (Uber JAR)
```bash
mvn package shade:shade
```

### Install to Local Maven Repository
```bash
mvn install
```

## Troubleshooting Commands

### Force Update Snapshots
```bash
mvn clean install -U
```

### Skip Specific Tests
```bash
mvn test -Dtest=!ShellTaskTest,!DockerTaskTest
```

### Run with Debug Logging
```bash
mvn clean install -X
```

### Check Effective POM
```bash
mvn help:effective-pom
```

### Validate POM Files
```bash
mvn validate
```

## Development Workflow

### Typical Development Cycle
```bash
# 1. Make code changes
# 2. Compile to check for errors
mvn compile

# 3. Run tests
mvn test

# 4. If all good, do full build
mvn clean install
```

### Quick Iteration (During Development)
```bash
# Compile only the module you're working on
mvn compile -pl scattersphere-core

# Test only that module
mvn test -pl scattersphere-core

# Or combine
mvn clean compile test -pl scattersphere-core
```

## CI/CD Script Commands

### Using Provided Scripts
```bash
# Build (as used in CI)
bash scripts/circle/build.sh

# Test (as used in CI)
bash scripts/circle/test.sh

# Generate Documentation
bash scripts/circle/doc.sh
```

## Scala-Specific Commands

### View Scala Compiler Options
```bash
mvn scala:help -Ddetail=true -Dgoal=compile
```

### Compile Only Scala Sources
```bash
mvn scala:compile
```

### Run Scala Console
```bash
mvn scala:console
```

## Docker Integration Testing

If you're testing Docker-related functionality:

```bash
# Ensure Docker is running
docker ps

# Run tests that include Docker tasks
mvn test -Dtest=DockerTaskTest
```

## Performance Testing

### Run with JVM Options
```bash
# Increase memory for tests
MAVEN_OPTS="-Xmx4g -Xms2g" mvn test

# Enable GC logging
MAVEN_OPTS="-Xlog:gc*:file=gc.log" mvn test
```

## Environment Variables

### Set Spark Master for Tests
```bash
# Use local Spark
SPARK_MASTER=local[*] mvn test

# Use cluster
SPARK_MASTER=spark://localhost:7077 mvn test
```

### Set Hadoop Home (if needed)
```bash
HADOOP_HOME=/ mvn test
```

## Quick Verification After Upgrade

Run this sequence to verify the upgrade was successful:

```bash
# 1. Clean everything
mvn clean

# 2. Compile all modules
mvn compile

# 3. Run all tests
mvn test

# 4. Package everything
mvn package

# 5. Install to local repository
mvn install
```

Expected output:
- All modules should compile without errors
- All tests should pass
- JAR files created in each module's target/ directory

## Common Issues and Solutions

### Issue: Maven can't find Scala dependencies
```bash
# Solution: Force update
mvn clean install -U
```

### Issue: Tests fail with ClassNotFoundException
```bash
# Solution: Clean and rebuild
mvn clean install
```

### Issue: Compilation errors about Scala version mismatch
```bash
# Solution: Clean Maven cache for this project
rm -rf ~/.m2/repository/io/buildfactory
mvn clean install
```

### Issue: Out of memory during build
```bash
# Solution: Increase Maven memory
export MAVEN_OPTS="-Xmx4g -Xms2g"
mvn clean install
```

## Useful Aliases

Add these to your `~/.zshrc` or `~/.bashrc`:

```bash
alias ssbuild='mvn clean install'
alias sstest='mvn test'
alias ssquick='mvn clean install -DskipTests'
alias ssclean='mvn clean && rm -rf ~/.m2/repository/io/buildfactory/scattersphere*'
```

---

**Note:** All commands assume you're in the project root directory:
`/Users/kenji/Development/scattersphere`

For more information, see:
- `SCALA_UPGRADE_2024.md` - Detailed upgrade documentation
- `MIGRATION_CHECKLIST.md` - Deployment verification checklist
- `UPGRADE_SUMMARY.md` - Quick reference guide

