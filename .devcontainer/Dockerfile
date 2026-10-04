# Keep version tags and their verified multi-platform digests in sync.
FROM composer:2.10.3@sha256:af98f42dfff7c68ba8d53c2164fd9fde1087b7d449514baa38c418b1f6bc4bac AS composer
FROM php:8.5.11-cli-trixie@sha256:19642e172d3a542225225e202ddc2c11f67bdcbddf147b676c49338609b9290f

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates git unzip libpq-dev libonig-dev libxml2-dev; \
    for extension in pdo_mysql pdo_pgsql pgsql mbstring simplexml dom xml xmlwriter pcntl; do \
        if ! php -r "exit(extension_loaded('$extension') ? 0 : 1);"; then \
            docker-php-ext-install -j2 "$extension" || exit 1; \
        fi; \
        php -r "exit(extension_loaded('$extension') ? 0 : 1);"; \
    done; \
    rm -rf /var/lib/apt/lists/*; \
    groupadd --gid 1000 vscode; \
    useradd --uid 1000 --gid vscode --create-home --shell /bin/bash vscode; \
    install -d -o vscode -g vscode \
        /workspaces/FromMySqlToPostgreSql \
        /home/vscode/.config/composer \
        /home/vscode/.cache/composer

ENV COMPOSER_HOME=/home/vscode/.config/composer \
    COMPOSER_CACHE_DIR=/home/vscode/.cache/composer

COPY --from=composer /usr/bin/composer /usr/local/bin/composer
COPY php.ini /usr/local/etc/php/conf.d/99-development.ini
COPY check-environment.php /usr/local/bin/check-dev-environment.php

WORKDIR /workspaces/FromMySqlToPostgreSql
USER vscode
RUN set -eux; \
    test -w /workspaces/FromMySqlToPostgreSql; \
    test -w "$COMPOSER_HOME"; \
    test -w "$COMPOSER_CACHE_DIR"; \
    php /usr/local/bin/check-dev-environment.php; \
    composer --version
CMD ["sleep", "infinity"]
