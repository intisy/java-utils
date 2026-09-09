# Java Utils

Archives containing JAR files are available as [releases](https://github.com/intisy/java-utils/releases).

## What is java-utils?

Utils for everything you need about java

## Usage in private projects

 * Maven (inside the `pom.xml` file)
```xml
  <repository>
      <id>github</id>
      <url>https://maven.pkg.github.com/intisy/java-utils</url>
      <snapshots><enabled>true</enabled></snapshots>
  </repository>
  <dependency>
      <groupId>io.github.intisy</groupId>
      <artifactId>java-utils</artifactId>
      <version>2.0.9</version>
  </dependency>
```

 * Maven (inside the `settings.xml` file)
```xml
  <servers>
      <server>
          <id>github</id>
          <username>your-username</username>
          <password>your-access-token</password>
      </server>
  </servers>
```

 * Gradle (inside the `build.gradle.kts` or `build.gradle` file)
```groovy
  repositories {
      maven {
          url "https://maven.pkg.github.com/intisy/java-utils"
          credentials {
              username = "<your-username>"
              password = "<your-access-token>"
          }
      }
  }
  dependencies {
      implementation 'io.github.intisy:java-utils:2.0.9'
  }
```

## Usage in public projects

 * Gradle (inside the `build.gradle.kts` or `build.gradle` file)
```groovy
  plugins {
      id "io.github.intisy.github-gradle" version "1.3.7"
  }
  dependencies {
      githubImplementation "intisy:java-utils:2.0.9"
  }
```

## License

[![Apache License 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
