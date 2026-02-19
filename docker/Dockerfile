############################################
# Stage 1 – Builder (FDWs + FreeTDS)
############################################
FROM postgres:18.2-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Tehran \
    FREETDS_VERSION=1.4.7

WORKDIR /build

# --------------------------
# Debian sources
# --------------------------
RUN rm -f /etc/apt/sources.list /etc/apt/sources.list.d/* && \
    printf "deb http://mirror.arvancloud.ir/debian bookworm main contrib non-free\n\
deb http://mirror.arvancloud.ir/debian-security bookworm-security main contrib non-free\n\
deb http://mirror.arvancloud.ir/debian bookworm-updates main contrib non-free\n" \
    > /etc/apt/sources.list

# --------------------------
# Build tools
# --------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    wget \
    gnupg2 \
    dirmngr \
    lsb-release \
    autoconf \
    automake \
    libtool \
    pkg-config \
    libssl-dev \
    libiconv-hook-dev \
    unixodbc-dev \
    default-libmysqlclient-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --------------------------
# PostgreSQL dev package
# --------------------------
RUN wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    | gpg --dearmor > /usr/share/keyrings/pgdg.gpg

RUN echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*

# =================================================
# Build FreeTDS
# =================================================
RUN wget https://www.freetds.org/files/stable/freetds-${FREETDS_VERSION}.tar.gz && \
    tar -xzf freetds-${FREETDS_VERSION}.tar.gz && \
    cd freetds-${FREETDS_VERSION} && \
    ./configure --prefix=/usr/local --with-openssl --enable-msdblib --enable-sybase-compat --disable-odbc && \
    make -j$(nproc) && make install && ldconfig

# =================================================
# Build mysql_fdw
# =================================================
RUN git clone --depth 1 https://github.com/EnterpriseDB/mysql_fdw.git && \
    cd mysql_fdw && make USE_PGXS=1 && make USE_PGXS=1 install

# =================================================
# Build tds_fdw
# =================================================
RUN git clone --depth 1 https://github.com/tds-fdw/tds_fdw.git && \
    cd tds_fdw && make USE_PGXS=1 FREETDS_CONFIG=/usr/local/bin/freetds-config && make USE_PGXS=1 install

############################################
# Stage 2 – Runtime PostgreSQL
############################################
FROM postgres:18.2-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Tehran

WORKDIR /app

# --------------------------
# Debian runtime dependencies
# --------------------------
RUN rm -f /etc/apt/sources.list /etc/apt/sources.list.d/* && \
    printf "deb http://mirror.arvancloud.ir/debian bookworm main contrib non-free\n\
deb http://mirror.arvancloud.ir/debian-security bookworm-security main contrib non-free\n\
deb http://mirror.arvancloud.ir/debian bookworm-updates main contrib non-free\n" \
    > /etc/apt/sources.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    openssl \
    cron \
    python3 \
    python3-pip \
    wget \
    gnupg2 \
    dirmngr \
    lsb-release \
    unixodbc \
    libssl3 \
    libiconv-hook1 \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# =================================================
# Install Microsoft ODBC Driver 18
# =================================================
RUN wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg

RUN echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/microsoft-prod.list

RUN apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# =================================================
# Copy FDWs + FreeTDS from builder
# =================================================
COPY --from=builder /usr/local /usr/local
COPY --from=builder /usr/lib/postgresql/18/lib/*mysql* /usr/lib/postgresql/18/lib/
COPY --from=builder /usr/lib/postgresql/18/lib/*tds* /usr/lib/postgresql/18/lib/
COPY --from=builder /usr/share/postgresql/18/extension/*mysql* /usr/share/postgresql/18/extension/
COPY --from=builder /usr/share/postgresql/18/extension/*tds* /usr/share/postgresql/18/extension/

RUN ldconfig

# =================================================
# FreeTDS config (tds_fdw)
# =================================================
RUN mkdir -p /etc/freetds
COPY ./configs/freetds.conf /etc/freetds/freetds.conf
ENV FREETDSCONF=/etc/freetds/freetds.conf

# --------------------------
# freetds.conf example
# --------------------------
# [global]
# tds version = 7.4
# client charset = UTF-8
# text size = 65536
#
# [sqlserver2022]
# host = sqlserver_host_or_ip
# port = 1433
# tds version = 7.4
# encrypt = require
# trust server certificate = yes

# =================================================
# Python packages
# =================================================
RUN pip3 install --break-system-packages \
    psycopg2-binary \
    pyyaml \
    -i https://pypi.tuna.tsinghua.edu.cn/simple

# =================================================
# App scripts
# =================================================
COPY scripts ./scripts
COPY configs ./configs
RUN chmod +x ./scripts/*.sh && touch /var/log/backup.log

# =================================================
# Cron
# =================================================
RUN echo "0 2 * * * /app/scripts/backup.sh >> /var/log/backup.log 2>&1" > /etc/cron.d/pg-backup
RUN chmod 0644 /etc/cron.d/pg-backup
RUN crontab /etc/cron.d/pg-backup

# =================================================
# Healthcheck
# =================================================
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
    CMD pg_isready -U $POSTGRES_USER -d $POSTGRES_DB || exit 1

CMD []
