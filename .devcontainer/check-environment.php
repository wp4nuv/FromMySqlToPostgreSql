<?php

declare(strict_types=1);

if (PHP_VERSION_ID < 80500 || PHP_VERSION_ID >= 80600) {
    throw new RuntimeException('The development environment requires PHP 8.5.');
}

foreach (['json', 'pdo', 'pdo_mysql', 'pdo_pgsql', 'pgsql', 'mbstring', 'simplexml',
    'dom', 'xml', 'xmlwriter', 'libxml', 'filter', 'tokenizer', 'phar', 'iconv', 'pcntl'] as $extension) {
    if (!extension_loaded($extension)) {
        throw new RuntimeException('Missing native PHP extension: ' . $extension);
    }
}

foreach (['proc_open', 'proc_terminate', 'pcntl_signal', 'pcntl_async_signals',
    'pg_put_line', 'pg_end_copy'] as $function) {
    if (!function_exists($function)) {
        throw new RuntimeException('Required worker/COPY function unavailable: ' . $function);
    }
}

echo 'PHP ', PHP_VERSION, ': native database, test and process requirements are available.', PHP_EOL;
