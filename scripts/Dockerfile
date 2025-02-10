FROM ubuntu:jammy

ARG DE

WORKDIR /engine

RUN apt update

ADD target/debian/de_${DE}_amd64.deb /engine

RUN apt install /engine/de_${DE}_amd64.deb -y