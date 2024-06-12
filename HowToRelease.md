# How to release

Instructions on how to release a new version of this project

## Build locally

```shell
mvn clean package
mvn -DskipITs=false verify
```

## Commit/Push all local changes to remote

```shell
git -am"/commit message/"
git push
```

## Commit/Push new version number

```shell
mvn versions:set
git -am"setting version X.Y"
git push
```

## Check github action build passes

https://github.com/smartrics/nifi-iotics/actions

## Tag code

```shell
git tag -a vX.Y -m"release X.Y"
git push origin tag vX.Y
```

## Create release on github from tag

https://github.com/smartrics/nifi-iotics/releases/new

- choose tag vX.Y
- write comment
- upload from local the nar file

## set new version as snapshot 

```shell
mvn versions:set
git -am"setting version X.Z-SNAPSHOT"
git push
```
