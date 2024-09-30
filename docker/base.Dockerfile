FROM ubuntu:latest
LABEL authors="loicmancino"
COPY ../src/commons/ .src/commons
COPY ../src/libs/ .src/libs

ENTRYPOINT ["top", "-b"]
