// swift-tools-version: 6.2
import PackageDescription

let swiftSettings: [SwiftSetting] = [
    .enableUpcomingFeature("ExistentialAny"),
]

let package = Package(
    name: "SwiftCache",
    platforms: [.macOS(.v15)],
    products: [
        // The client library applications depend on.
        .library(name: "SwiftCache", targets: ["SwiftCache"]),
        .executable(name: "swiftcache-server", targets: ["swiftcache-server"]),
        .executable(name: "swiftcache-sim", targets: ["swiftcache-sim"]),
        .executable(name: "swiftcache-cli", targets: ["swiftcache-cli"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.65.0"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.3.0"),
    ],
    targets: [
        // Shared, dependency-free building blocks: types, wire protocol, hashing.
        .target(name: "SwiftCacheCore", swiftSettings: swiftSettings),

        // Server logic as deterministic, message-driven state machines.
        // No I/O, no clocks, no threads: everything goes through `NodeEnvironment`.
        .target(name: "SwiftCacheServer", dependencies: ["SwiftCacheCore"], swiftSettings: swiftSettings),

        // Deterministic simulator + workloads that drive the server state machines.
        .target(name: "SwiftCacheSim", dependencies: ["SwiftCacheCore", "SwiftCacheServer", "SwiftCache"], swiftSettings: swiftSettings),

        // Real networking (SwiftNIO): framing, connections, hosting state machines on an event loop.
        .target(
            name: "SwiftCacheNet",
            dependencies: [
                "SwiftCacheCore", "SwiftCacheServer",
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOPosix", package: "swift-nio"),
            ],
            swiftSettings: swiftSettings
        ),

        // The client library.
        .target(
            name: "SwiftCache",
            dependencies: ["SwiftCacheCore", "SwiftCacheNet"],
            swiftSettings: swiftSettings
        ),

        .executableTarget(
            name: "swiftcache-server",
            dependencies: [
                "SwiftCacheCore", "SwiftCacheServer", "SwiftCacheNet",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            swiftSettings: swiftSettings
        ),
        .executableTarget(
            name: "swiftcache-sim",
            dependencies: [
                "SwiftCacheSim",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            swiftSettings: swiftSettings
        ),
        .executableTarget(
            name: "swiftcache-cli",
            dependencies: [
                "SwiftCache",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
            ],
            swiftSettings: swiftSettings
        ),

        .testTarget(name: "SwiftCacheCoreTests", dependencies: ["SwiftCacheCore", "SwiftCacheServer"], swiftSettings: swiftSettings),
        .testTarget(name: "SwiftCacheSimTests", dependencies: ["SwiftCacheSim"], swiftSettings: swiftSettings),
        .testTarget(name: "SwiftCacheClientTests", dependencies: ["SwiftCache"], swiftSettings: swiftSettings),
        .testTarget(
            name: "SwiftCacheNetTests",
            dependencies: ["SwiftCacheNet", .product(name: "NIOEmbedded", package: "swift-nio")],
            swiftSettings: swiftSettings
        ),
    ]
)
