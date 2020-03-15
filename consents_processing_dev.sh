#!/bin/bash
docker pull dtok/water-data-allocation:dev
docker run --name wdc1 -v /home/mike/git/WaterDataConsents/parameters-dev.yml:/parameters.yml dtok/water-data-allocation:dev
docker rm wdc1