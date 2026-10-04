#!/bin/sh
set -eu
if [ ! -f .env ]; then
    cp .env.example .env
    chmod 600 .env
    echo 'Created .env with local development defaults. Customize passwords before sharing this environment.'
fi
