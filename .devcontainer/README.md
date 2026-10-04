# PHP 8.5 development environment

Reopen this repository in a Dev Containers-compatible editor. The initialize
hook creates an ignored `.env` from `.env.example` only when `.env` is absent.
The example passwords are local development defaults; customize your local file.
Existing credentials are never overwritten by the hook.

Compose builds the Dockerfile and starts MySQL and PostgreSQL. The container
stays running, waits for both database health checks, and opens the repository at
`/workspaces/FromMySqlToPostgreSql` as the non-root `vscode` user. Creation installs
locked Composer dependencies, validates them, and checks platform requirements.

For Docker Compose without an editor:

```sh
sh .devcontainer/initialize.sh
docker compose up --build -d --wait
docker compose exec devcontainer sh .devcontainer/post-create.sh
docker compose exec devcontainer php .devcontainer/smoke-databases.php
```

The smoke check uses temporary tables and rolls back its COPY transaction. It
also opens independent connections from two PHP child processes, exercising the
runtime needed by the proposed process pool. It does not migrate application
fixtures or prove the refactor's job/recovery guarantees.

## Runtime and database choices

- PHP 8.5.11 CLI on Debian Trixie; Composer 2.10.3 copied from its official image.
  Both base images use verified multi-platform digests. Update the tag and digest
  together when upgrading, then rebuild and run the environment/database checks.
- Native PDO MySQL, PDO PostgreSQL, pgsql, mbstring and SimpleXML; DOM/XML,
  XMLWriter, tokenizer and the other extensions required by development tools.
- `proc_open` and PCNTL for independent processes and signal handling; Compose
  uses an init process to reap children. PHP displays E_ALL diagnostics on stderr.
- MySQL 8.4.11 and PostgreSQL 18.6. Update pins through a build and database smoke
  check. Versioned volumes keep these databases separate from legacy volumes;
  PostgreSQL 18 mounts `/var/lib/postgresql` for its versioned data directory.

The Dockerfile stops immediately on extension compilation failures and checks
each native extension after installation. Its final build checks run as `vscode`,
with writable workspace, Composer configuration and download-cache directories.
Composer dependencies remain in the mounted repository and are installed from
the lockfile by the creation hook; they are not baked into the development image.
Compose supplies the init process for worker reaping; use `docker run --init` for
standalone worker experiments with this image.

Inside the workspace, database hostnames are `mysql:3306` and `pgsql:5432`.
Host ports are not published. Connection variables are inherited by child
processes; the MySQL root password is supplied only to the database service.
MySQL creates the configured source database and migration user; PostgreSQL
creates the configured target database/user.

The legacy entry point still needs JSON/XML DSNs with these service hostnames
and your local credentials. Its configuration loader does not expand environment
references yet. Planned `bin/migrate` and worker commands are not implemented.

PHP uses a 512 MB limit per process, so account for each worker plus the
coordinator when sizing Docker resources. Xdebug and GD are not installed because
neither is required by the CLI migration or current development packages.
Spatial/PostGIS support needs a separate tested image and fixtures.

## Persistence and shutdown

Use `docker compose down` to stop the services while retaining data. Dev Container
shutdown stops the Compose services too. Database initialization variables apply
only to empty volumes: changing `.env` does not change existing database users
or passwords. Perform password/database changes explicitly, or use a separate
Compose project for new disposable data. Never reuse a data volume across major
database versions without an explicit upgrade procedure.

The prior tracked `.env` has been converted to valid dotenv syntax locally and
removed from version control; `.env.example` documents the new variables.
Older `mysql-data`/`postgres-data` volumes are untouched and not automatically
imported. The existing application parse error at
`FromMySqlToPostgreSql.php:577` remains a separate refactor blocker.
