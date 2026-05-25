FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential cmake git curl zip unzip tar pkg-config \
    autoconf automake libtool \
    python3 \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh

ENV VCPKG_ROOT=/opt/vcpkg

WORKDIR /app
COPY . .

RUN cmake -B build -S . \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --config Release -j$(nproc) --target jepsen_worker trans_db_replica

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    libstdc++6 python3 iptables iproute2 net-tools \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/build/jepsen_worker /usr/local/bin/
COPY --from=builder /app/build/trans_db_replica /usr/local/bin/
WORKDIR /workspace
