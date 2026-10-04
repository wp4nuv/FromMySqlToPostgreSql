<?php

declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

function environment(string $name): string
{
    $value = getenv($name);
    if ($value === false || $value === '') {
        throw new RuntimeException('Missing environment variable: ' . $name);
    }
    return $value;
}

function connectPdo(string $driver, string $prefix, string $databaseKey): PDO
{
    return new PDO(
        sprintf('%s:host=%s;port=%s;dbname=%s', $driver, environment($prefix . '_HOST'),
            environment($prefix . '_PORT'), environment($databaseKey)),
        environment($prefix . '_USER'),
        environment($prefix . '_PASSWORD'),
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
    );
}

$mysql = connectPdo('mysql', 'MYSQL', 'MYSQL_DATABASE');
$postgres = connectPdo('pgsql', 'POSTGRES', 'POSTGRES_DB');
if ($mysql->query('SELECT 1')->fetchColumn() != 1 || $postgres->query('SELECT 1')->fetchColumn() != 1) {
    throw new RuntimeException('Database query failed.');
}

if (($argv[1] ?? '') === '--worker') {
    echo 'Independent worker database connections passed.', PHP_EOL;
    exit(0);
}

// Libpq reads credentials from the process environment, never command arguments.
foreach (['PGHOST' => 'POSTGRES_HOST', 'PGPORT' => 'POSTGRES_PORT',
    'PGDATABASE' => 'POSTGRES_DB', 'PGUSER' => 'POSTGRES_USER',
    'PGPASSWORD' => 'POSTGRES_PASSWORD'] as $libpqKey => $environmentKey) {
    putenv($libpqKey . '=' . environment($environmentKey));
}
$connection = pg_connect('', PGSQL_CONNECT_FORCE_NEW);
if ($connection === false) {
    throw new RuntimeException('Native PostgreSQL connection failed.');
}
pg_query($connection, 'BEGIN');
try {
    pg_query($connection, 'CREATE TEMP TABLE container_copy_check (id integer, value text) ON COMMIT DROP');
    pg_query($connection, 'COPY container_copy_check (id, value) FROM STDIN');
    if (!pg_put_line($connection, "1\tcopy-ready\n\\.\n") || !pg_end_copy($connection)) {
        throw new RuntimeException('Native PostgreSQL COPY failed.');
    }
    $result = pg_query($connection, 'SELECT value FROM container_copy_check WHERE id = 1');
    if ($result === false || pg_fetch_result($result, 0, 0) !== 'copy-ready') {
        throw new RuntimeException('COPY round trip did not preserve the value.');
    }
} finally {
    pg_query($connection, 'ROLLBACK');
    pg_close($connection);
}

$workers = [];
try {
    for ($i = 0; $i < 2; ++$i) {
        $worker = new Symfony\Component\Process\Process([PHP_BINARY, __FILE__, '--worker']);
        $worker->setTimeout(30);
        $worker->start();
        $workers[] = $worker;
    }
    foreach ($workers as $worker) {
        if ($worker->wait() !== 0) {
            throw new RuntimeException('Independent worker database check failed.');
        }
    }
} finally {
    foreach ($workers as $worker) {
        if ($worker->isRunning()) {
            $worker->stop(1);
        }
    }
}

echo 'PDO MySQL, PDO PostgreSQL, transactional COPY and two independent workers passed.', PHP_EOL;
