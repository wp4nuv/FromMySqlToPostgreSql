#!/bin/sh
set -eu
php /usr/local/bin/check-dev-environment.php
composer install --prefer-dist --no-progress --no-interaction
composer validate --strict
composer check-platform-reqs
