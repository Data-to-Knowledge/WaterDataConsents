#!/bin/bash
docker pull dtok/water-data-consents:dev
docker run -v /media/waterdata1/git/WaterDataConsents/parameters-dev.yml:/parameters.yml dtok/water-data-consents:dev