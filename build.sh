#!/bin/sh

echo Building serger89/chcustom:build

docker build -t serger89/chcustom:build .

docker container create --name extract serger89/chcustom:build
docker container cp extract:/app/ClickhouseCustomMetrics ./build
docker container rm -f extract

echo Building finnished