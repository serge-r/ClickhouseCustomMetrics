# Build main package
FROM ubuntu:latest AS build_base
RUN apt-get update
RUN apt-get install -y wget git gcc make binutils
RUN wget -P /tmp https://golang.org/dl/go1.16.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf /tmp/go1.16.linux-amd64.tar.gz
RUN rm /tmp/go1.16.linux-amd64.tar.gz
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"
RUN mkdir -p "/tmp/build"
WORKDIR /tmp/build
COPY . .
RUN go build -ldflags="-s -w" -o ./ClickhouseCustomMetrics .

# Start new from a smaller image
FROM alpine:latest
COPY --from=build_base /tmp/build/ClickhouseCustomMetrics /app/ClickhouseCustomMetrics
CMD ["/app/ClickhouseCustomMtrics"]