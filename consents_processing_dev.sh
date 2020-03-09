#!/bin/bash
docker pull dtok/water-data-allocation:dev
docker run --name wdc1 -v /media/waterdata1/git/WaterDataConsents/parameters-dev.yml:/parameters.yml dtok/water-data-allocation:dev
docker rm wdc1