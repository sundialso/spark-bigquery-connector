## To build the JAR, run the following commands.
## Note that you need to use JDK 1.8 to build
## Check if your Java 8 exists by running - `/usr/libexec/java_home -v1.8`
## and set JAVA_HOME to it.
```bash
export JAVA_HOME=<path to JDK>
export PATH=$JAVA_HOME/bin:$PATH
./mvnw clean install -Pdsv2_3.5 -DskipTests
```
