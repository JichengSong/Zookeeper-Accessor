# Zookeeper-Accessor

README version 0.1.0

[![Build Status](https://drone.io/github.com/ZheYuan/Zookeeper-Accessor/status.png)](https://drone.io/github.com/ZheYuan/Zookeeper-Accessor/latest)

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

`git clone https://github.com/ZheYuan/Zookeeper-Accessor.git`

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

## Using Zookeeper-Accessor

### Using ZkConfig Class

`default.config.path` is used to point out config file directory.

When use Accessor with config file in `./conf`, we can run class:

`java -Ddefault.config.path=./conf`

and write one line code:

`ZkConfig config = new ZkConfig();`

Or you can use ZkConfig provide publish method to set attribute.

### Using Accessor Class

This is a singleton client object of zookeper.

First call with a vaild `ZkConfig` object:

`Accessor accessor = Accessor.getInstance(config);`

And then if wanna get the lastest singleton in somewhere, just call with a `NULL`:

`Accessor accessor2 = Accessor.getInstance(NULL);`

### Using Publish Class

This class is a service instance use ephemeral node of zookeeper. Each Publish object only publish once, or it will throws an `OperationNotSupportedException`.

```
Publish publish = new Publish("test.service", "1", "0",
                            "PublishTestHandle", "127.0.0.1:80".getBytes());
accessor.publishService(publish);
// Remove the service
publish.die();
// This object's life cycle is same to accessor.
accessor.publishService(new Publish("test.service", "1", "0", "PublishTest", "127.0.0.1:81".getBytes());
```

### Using Subscribe Class

This class is a client instance. Each Subscribe objcet only subcribe once, or it will throws an `OperationNotSupportedException`.

Subscribe class is same to Publish class.

## License

Zookeeper-Accessor is under the gpl v3.0 license. See the [LICENSE][license] file for details.

[license]: https://github.com/ZheYuan/Zookeeper-Accessor/blob/master/LICENSE

