#!/bin/bash
docker run -p 8888:8888 -i -t -v "$(pwd)"/.:/src bigdata /bin/bash