## Build and Run

To simply run the source code from within, e.g., Eclipse make sure to add
`--add-modules=jdk.incubator.httpclient` as a VM argument. Otherwise you get a
`NoClassDefFoundError` exception.

We use Gradle as a build tool. In order to build the executeables run 
`./gradlew build` on Linux/macOS or `./gradlew.bat build` on Windows. This will
create the file `build/distributions/titanccp-aggregation.tar` which contains
start scripts for Linux/macOS and Windows.

This repository also contains a Dockerfile. Run
`docker build -t titan-ccp-aggregation .` to create a container from it.