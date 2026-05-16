FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y \
    build-essential cmake git curl zip unzip tar pkg-config \
    autoconf automake libtool \
    && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && /opt/vcpkg/bootstrap-vcpkg.sh

ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="${VCPKG_ROOT}:${PATH}"

WORKDIR /app
COPY . .

RUN cmake -B build -S . \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --config Release -j$(nproc)

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/build/trans_db_server /usr/local/bin/
COPY --from=builder /app/build/bench_tpcc /usr/local/bin/

EXPOSE 5432
ENTRYPOINT ["trans_db_server"]
CMD ["--port", "5432", "--shards", "3", "--data-dir", "/data"]
