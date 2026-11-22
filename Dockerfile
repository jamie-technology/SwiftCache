# syntax=docker/dockerfile:1.7
# ---- build ----
FROM swift:6.4-noble AS build
WORKDIR /src
# Resolve dependencies first so they cache independently of source changes.
COPY Package.swift Package.resolved* ./
RUN --mount=type=cache,target=/src/.build,id=swiftcache-build swift package resolve
COPY Sources ./Sources
COPY Tests ./Tests
RUN --mount=type=cache,target=/src/.build,id=swiftcache-build \
    swift build -c release --static-swift-stdlib \
      --product swiftcache-server && \
    swift build -c release --static-swift-stdlib --product swiftcache-cli && \
    swift build -c release --static-swift-stdlib --product swiftcache-sim && \
    mkdir -p /out && \
    cp .build/release/swiftcache-server .build/release/swiftcache-cli .build/release/swiftcache-sim /out/

# ---- runtime ----
FROM ubuntu:24.04
RUN apt-get update -q && apt-get install -yq --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home swiftcache
COPY --from=build /out/ /usr/local/bin/
USER 10001:10001
EXPOSE 7400 7500
ENTRYPOINT ["/usr/local/bin/swiftcache-server"]
