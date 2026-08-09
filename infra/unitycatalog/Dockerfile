# Unity Catalog with MINT-ON-START.
#
# Verified behaviour (UC v0.5.0): UC reads its storage credential ONCE, at
# startup, and vends that boot-time MinIO STS session for the life of the
# process -- it does NOT hot-reload server.properties. So the only safe moment
# to install a fresh credential is immediately BEFORE the server starts.
#
# This image wraps UC's entrypoint so that EVERY (re)start -- including a
# crash-triggered `restart: unless-stopped` -- first mints a fresh, bucket-scoped
# MinIO STS session and injects it into server.properties, then execs the
# server. Consequently UC can never come up holding an EXPIRED token, and a
# restart is self-healing instead of resurrecting stale creds.
#
# This mirrors real cloud, where UC self-vends per-request via its own instance
# IAM role; the local mint-on-start is the equivalent guarantee. It also folds
# in (and replaces) the old one-shot `uc-rotate` seeder.
#
# Built FROM the UC image (Alpine) to inherit bin/start-uc-server + the JRE; we
# add aws-cli (STS AssumeRole against MinIO) and su-exec (drop back to the
# non-root `unitycatalog` user to actually run the server).
ARG UC_VERSION=v0.5.0
FROM unitycatalog/unitycatalog:${UC_VERSION}

USER root
RUN apk add --no-cache aws-cli su-exec bash

# bin/start-uc-server resolves `java` via a login shell profile; su-exec spawns a
# non-login process, so make Java resolvable unconditionally (JAVA_HOME is set by
# the base image).
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH="/usr/lib/jvm/default-jvm/bin:${PATH}"

COPY uc-entrypoint.sh /entrypoint/uc-entrypoint.sh
RUN chmod +x /entrypoint/uc-entrypoint.sh

WORKDIR /home/unitycatalog
ENTRYPOINT ["/bin/bash", "/entrypoint/uc-entrypoint.sh"]
