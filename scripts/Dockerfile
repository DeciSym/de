FROM ubuntu:jammy

ARG VERSION

WORKDIR /engine

ADD target/debian/de_${VERSION}_amd64.deb /engine

RUN apt install /engine/de_${VERSION}_amd64.deb -y
RUN rm -f apt install /engine/de_${VERSION}_amd64.deb

ENTRYPOINT ["de"]