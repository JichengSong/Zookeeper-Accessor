# Zookeeper-Accessor

README version 0.1.0

A common accessor of zookeeper service implements with java. 
Zookeeper-Accessor was made up of this components:

* Publish: puslish a service with path /root(in config file)/service.id/version/sharding/endpoint:value.
* Subscribe: subcribe a service with path /root(in config file)/service.id/version/sharding.
* Accessor: client of zookeeper and auto reconnet zookeeper when recive exceptions.

## Contact

- Email: yizer16[at]163[dot]com, yizer16[at]gmail[dot]com

## Getting Started

### Getting Zookeeper-Accessor

The latest release code is available at [Github][github-release].

[github-release]: https://github.com/ZheYuan/Zookeeper-Accessor/releases

### Building

You can build Zookeeper-Accessor from source:

You need setup maven to build this project:
<http://maven.apache.org/>

Run this command and get the source:

```
git clone https://github.com/ZheYuan/Zookeeper-Accessor.git
```

Enter workcopy and install to local:

```
cd Zookeeper-Accessor
mvn install
```

Or

```
cd Zookeeper-Accessor
mvn assembly:assembly
```
This will generate two jars in `./target` directory.

### Using Zookeeper-Accessor


