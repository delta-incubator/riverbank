# Rust as the base image
FROM rust:1.54 as build

# Create a new empty shell project
RUN USER=root cargo new --bin riverbank
WORKDIR /riverbank

# Copy our manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# Build only the dependencies to cache them
RUN cargo build --release
RUN rm src/*.rs

# Copy the source code
COPY ./src ./src
COPY ./sqlx-data.json ./sqlx-data.json

# Build for release.
RUN rm ./target/release/deps/riverbank*
RUN cargo build --release

# The final base image
FROM debian:buster-slim

WORKDIR /usr/src

# Copy from the previous build
COPY --from=build /riverbank/target/release/riverbank /usr/src/riverbank

COPY ./config.yml /usr/src/config.yml
COPY ./apidocs /usr/src/apidocs
COPY ./migrations /usr/src/migrations
COPY ./views /usr/src/views

RUN apt-get update && apt-get install -y libssl-dev wait-for-it
# Run the binary
CMD ["/usr/src/riverbank"]