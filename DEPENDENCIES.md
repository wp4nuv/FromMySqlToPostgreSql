# Dependencies for the PHP 8.5 refactor

The proposed `REFACTOR_PLAN.md` keeps this project a CLI migration tool with
PostgreSQL COPY and independent PHP worker processes. Composer targets PHP 8.5
patch releases (`~8.5.0`); PHP 8.6 requires a separate compatibility review.

## Runtime packages

| Package | Purpose |
| --- | --- |
| Monolog 3 and PSR Log 3 | Existing logging and future structured run/job/worker context |
| Symfony Console 7.4 | Planned coordinator/worker commands, option parsing, progress and exit status |
| Symfony OptionsResolver 7.4 | Configuration defaults, required options, types and allowed values after JSON/XML normalization |
| Symfony Process 7.4 | Independent worker startup, output handling, timeouts and shutdown |

Symfony 7.4 is the maintained LTS line. These components are preparation for the
refactor; adding them does not implement commands, concurrency or validation.
Process output must still be drained continuously and retained output bounded.

Unused Twig, Guzzle PSR-7, League, Laminas, Doctrine and Symfony ErrorHandler
requirements were removed after reviewing all repository PHP entry points.
Migration continues to use native PDO and ext-pgsql rather than an ORM.

Composer now declares both PDO database drivers, pgsql, mbstring and SimpleXML,
as required by the existing code. The devcontainer must supply these extensions;
adding Composer requirements does not install extensions.

## Development packages

- PHPUnit 13 for the planned unit and database integration assertions.
- PHPStan 2 with strict and deprecation rules for typed boundaries and migration issues.
  Include each extension's `rules.neon` in the future PHPStan configuration.
- PHP_CodeSniffer 4 for coding standards.

PHPMD was removed: stable 2.15 emits deprecated-cast warnings on PHP 8.5 and its
PDepend parser documents syntax support through PHP 8.3. Use PHPStan and native
PHP lint for modernized code rather than suppressing these warnings.

## Reproducible installation

Keep `composer.lock` in version control. On PHP 8.5 with all required extensions:

```sh
composer install --prefer-dist --no-progress --no-interaction
composer validate --strict
composer check-platform-reqs
composer audit
```

Use `composer update --with-all-dependencies` for intentional upgrades and review
the lockfile diff. CI installs locked versions and checks platform requirements
and security advisories. Application lint, static analysis and integration tests
remain refactor acceptance gates; the existing SQL fixtures are not an automated
PHPUnit suite yet.

The placeholder package description has been corrected. License metadata remains
unchanged pending the plan's provenance review of the GPL/proprietary mismatch.

Dependency installation, strict Composer validation, platform checks, audit and
runtime-package smoke checks passed on PHP 8.5.11. Existing application lint fails
at `migration/FromMySqlToPostgreSql/FromMySqlToPostgreSql.php:577` (`[]` used for
reading), as identified in the plan. Dependency changes do not repair that blocker
or establish database migration correctness.
