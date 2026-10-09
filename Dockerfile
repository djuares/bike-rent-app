FROM rust:latest

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        gdb \
        lldb \
        less \
        git \
        sudo \
        netcat-openbsd \
        procps \
        iproute2 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add clippy rustfmt rust-src \
    && cargo install cargo-watch --locked

# Crea un usuario `dev` cuyo UID/GID se ajusta al host vía build args. Así los
# archivos generados por cargo (Cargo.lock, target/) son del usuario host y no
# de root. Los UID/GID por defecto coinciden con el usuario más común (1000).
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd --gid ${USER_GID} dev \
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m -s /bin/bash dev \
    && echo 'dev ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && chown -R dev:dev /usr/local/cargo /usr/local/rustup

USER dev

WORKDIR /workspace

CMD ["/bin/bash"]
