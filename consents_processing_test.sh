#!/bin/bash
docker pull dtok/water-data-allocation:test
docker rm wdc_test
docker run --name wdc_test -v /home/mike/git/WaterDataConsents/parameters-test.yml:/parameters.yml dtok/water-data-allocation:test
echo "Success!"