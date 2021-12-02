# Titan Control Center - History

The [Titan Control Center](https://doi.org/10.1016/j.simpa.2020.100050)
is a scalable monitoring infrastructure for [Industrial DevOps](https://industrial-devops.org/).
It allows to monitor, analyze and visualize the electrical power consumption of
devices and machines in industrial production environments.

This repository contains the **History** microservice of the Titan Control Center.

## Build and Run

We use Gradle as a build tool. In order to build the executeables run 
`./gradlew build` on Linux/macOS or `./gradlew.bat build` on Windows. This will
create the file `build/distributions/titanccp-history.tar` which contains
start scripts for Linux/macOS and Windows.

This repository also contains a Dockerfile. Run
`docker build -t titan-ccp-history .` to create a container from it (after
building it with Gradle).

## Reference

Please cite the Titan Control Center as follows:

> S. Henning, W. Hasselbring, *The Titan Control Center for Industrial DevOps analytics research*, Software Impacts 7 (2021), DOI: [10.1016/j.simpa.2020.100050](https://doi.org/10.1016/j.simpa.2020.100050).

BibTeX:

```bibtex
@article{Henning2021,
    title = {The Titan Control Center for Industrial DevOps analytics research},
    journal = {Software Impacts},
    volume = {7},
    pages = {100050},
    year = {2021},
    doi = {10.1016/j.simpa.2020.100050},
    author = {Sören Henning and Wilhelm Hasselbring},
}
```
