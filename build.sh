#!/bin/bash
mvn clean install
docker build -t pack .
