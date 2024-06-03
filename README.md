# NiFi IOTICS

Processors and service to integrate NiFi and IOTICSpace

## Build and test

`mvn clean package`

A successful build creates a `nar` file in `nifi-iotics-nar\target` with the services and processors. 

To run the integration tests, you need to run the `verify` target as following

`mvn -DskipITs=false verify`

Integration tests need the file `nifi-iotics-processor/.env` with the following structure:

```properties
hostDNS=<your IOTICSpace dns (e.g. myspace.iotics.com) >
agentKey=<your agent key name>
userKey=<your user key name>
seed=<a seed>
tokenDuration=<token duration in seconds>
```

