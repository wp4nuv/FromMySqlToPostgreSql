<?php

namespace App;

/*
 * This file is a part of "FromMySqlToPostgreSql" - the database migration tool.
 *
 * Copyright 2015 Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).
 * If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

use Exception;
use PDO;
use PDOException;
use App\ViewGenerator as ViewGenerator;
use App\MapDataTypes as MapDataTypes;

/**
 * This class performs structure and data migration from MySql database to PostgreSql database.
 *
 * @author Anatoly Khaytovich
 */

/**
 * FromMySqlToPostgreSQL
 *
 * @property \App\Utilities $Utility
 * @property \App\Logs $Log
 */
class FromMySqlToPostgreSql
{
    /**
     * A \PDO instance, connected to MySql server.
     *
     * @var \PDO
     */
    private PDO $mysql;

    /**
     * A \PDO instance, connected to PostgreSql server.
     *
     * @var \PDO
     */
    private PDO $pgsql;

    /**
     * A \resource instance, connected to PostgreSql server for data loading via copy.
     *
     * @var \resource
     */
    private $pgSqlData;

    /**
     * Encoding of target (PostgreSql) server.
     *
     * @var string
     */
    private string $dbEncoding;

    /**
     * A schema name.
     *
     * @var string
     */
    private string $dbSchema;

    /**
     * MySql connection string.
     *
     * @var string
     */
    private string $sourceConString;

    /**
     * PostgreSql connection string.
     *
     * @var string
     */
    private string $targetConString;

    /**
     * A name of MySql database, that will be migrated.
     *
     * @var string
     */
    private string $mySqlDbName;

    /**
     * An array of MySql tables, that need to be migrated.
     *
     * @var array
     */
    private array $tablesToMigrate;

    /**
     * An array of MySql views, that need to be migrated.
     *
     * @var array
     */
    private array $viewsToMigrate;

    /**
     * Path to temporary directory.
     *
     * @var string
     */
    private string $tempDirectory;

    /**
     * Summary report array.
     *
     * @var array
     */
    private array $summaryReport;

    /**
     * Path to "not_created_views" directory.
     *
     * @var string
     */
    private string $viewErrorsDirPath;

    /**
     * During migration each table's data will be split into chunks of $floatDataChunkSize.
     *
     * @var float
     */
    private float $dataChunkSize;

    /**
     * Flag, indicating that only data should migrate
     *
     * @var bool
     */
    private bool $isDataOnly;

    /**
     * @var string
     */
    private string $sql;



    /**
     * Constructor.
     *
     * @param array $arrConfig
     * @return void
     */
    public function __construct(array $arrConfig)
    {
        if (!extension_loaded('pgsql')) {
            echo "Postgresql not enabled: you need the 'pgsql' module.\n";
            return;
        }
        if (!extension_loaded('pdo_mysql')) {
            echo "Postgresql not enabled: you need the 'pdo_mysql' module.\n";
            return;
        }
        if (!extension_loaded('pdo_pgsql')) {
            echo "Postgresql not enabled: you need the 'pdo_pgsql' module.\n";
            return;
        }
        if (!extension_loaded('mbstring')) {
            echo "Multibyte extension not loaded: you need the 'mbstring' module.\n";
            return;
        }
        if (ini_get('register_argc_argv') == 0) {
            echo "register_argc_argv is not turned on, we can't process command line arguments.\n";
            return;
        }

        if (!isset($arrConfig['config']['source'])) {
            echo PHP_EOL, '-- Cannot perform a migration due to missing source database (MySql) 
            connection string.', PHP_EOL,
            '-- Please, specify source database (MySql) connection string, and run the tool again.', PHP_EOL;

            return;
        }

        if (!isset($arrConfig['config']['target'])) {
            echo PHP_EOL, '-- Cannot perform a migration due to missing target 
            database (PostgreSql) connection string.', PHP_EOL,
            '-- Please, specify target database (PostgreSql) connection string, and run the tool again.', PHP_EOL;

            return;
        }

        $this->setDefaults($arrConfig);

        $this->createDefaultAssets();
        $this->Utility = new Utilities($this);
        $this->Log = new Logs();
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
    }

    /**
     * Check if both servers are connected.
     * If not, then create connections.
     *
     * @return void
     */
    private function connect()
    {
        if (empty($this->mysql)) {
            $arrSrcInput = explode(',', $this->sourceConString, 3);
            $this->mysql = new PDO($arrSrcInput[0], $arrSrcInput[1], $arrSrcInput[2]);
            $this->mysql->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            $this->mysql->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->Log->write('Connected to MySQL', 'common');
        }

        if (empty($this->pgsql)) {
            $arrDestInput = explode(',', $this->targetConString, 3);
            $this->pgsql = new PDO($arrDestInput[0], $arrDestInput[1], $arrDestInput[2]);
            $this->pgsql->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            $this->pgsql->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            // These are poor man's replacements to avoid 2 connection strings at this point in time.
            $dataDsn = str_replace('pgsql:', '', $arrDestInput[0]);
            $dataDsn = str_replace(';', ' ', $dataDsn);
            $this->pgSqlData = pg_connect($dataDsn . " user=" . $arrDestInput[1] . " password=" . $arrDestInput[2]);
            if (!$this->pgSqlData) {
                echo pg_last_error();

                return;
            }
            pg_query($this->pgSqlData, "SET synchronous_commit=off");
        }
    }

    /**
     * Load MySql tables, that need to be migrated into an array.
     *
     * @return bool
     */
    private function loadStructureToMigrate(): bool
    {
        $boolRetVal = false;

        try {
            $this->connect();
            $this->sql = 'SHOW FULL TABLES IN `' . $this->mySqlDbName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrResult = $stmt->fetchAll(PDO::FETCH_ASSOC);

            foreach ($arrResult as $arrRow) {
                if ('BASE TABLE' == $arrRow['Table_type']) {
                    $this->tablesToMigrate[] = $arrRow;
                } elseif ('VIEW' == $arrRow['Table_type']) {
                    $this->viewsToMigrate[] = $arrRow;
                }
                unset($arrRow);
            }

            $boolRetVal = true;
            unset($this->sql, $stmt, $arrResult);
        } catch (PDOException $exception) {
            $this->Log->generateError(
                $exception,
                __METHOD__ . PHP_EOL . "\t" . '-- Cannot load tables/views from source (MySql) database...',
                $this->sql
            );
        }

        return $boolRetVal;
    }

    /**
     * Create a new database schema.
     * Insure a uniqueness of a new schema name.
     *
     * @return bool
     */
    private function createSchema(): bool
    {
        $boolRetVal = false;
        $boolSchemaExists = false;
        $this->sql = '';

        try {
            $this->connect();

            if (empty($this->dbSchema)) {
                $this->dbSchema = $this->mySqlDbName;

                for ($i = 1; true; $i++) {
                    $this->sql = "SELECT schema_name FROM information_schema.schemata "
                        . "WHERE schema_name = '" . $this->dbSchema . "';";

                    $stmt = $this->pgsql->query($this->sql);
                    $arrSchemas = $stmt->fetchAll(PDO::FETCH_ASSOC);

                    if (empty($arrSchemas)) {
                        unset($this->sql, $arrSchemas, $stmt);
                        break;
                    } elseif (1 == $i) {
                        $this->dbSchema .= '_' . $i;
                        unset($this->sql, $arrSchemas, $stmt);
                    } else {
                        $arrSchema = explode('_', $this->dbSchema);
                        $arrSchema[count($arrSchema) - 1] = $i;
                        $this->dbSchema = implode('_', $arrSchema);
                        unset($this->sql, $arrSchemas, $stmt, $arrSchema);
                    }
                }
            } else {
                $this->sql = "SELECT schema_name FROM information_schema.schemata "
                    . "WHERE schema_name = '" . $this->dbSchema . "';";

                $stmt = $this->pgsql->query($this->sql);
                $arrSchemas = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $boolSchemaExists = !empty($arrSchemas);
                unset($this->sql, $arrSchemas, $stmt);
            }

            if (!$boolSchemaExists) {
                $this->sql = 'CREATE SCHEMA "' . $this->dbSchema . '";';
                $stmt = $this->pgsql->query($this->sql);
                unset($this->sql, $stmt);
            }

            $boolRetVal = true;
        } catch (PDOException $exception) {
            $this->Log->generateError(
                $exception,
                __METHOD__ . PHP_EOL . "\t" . '-- Cannot create a new schema...',
                $this->sql
            );
        }

        return $boolRetVal;
    }

    /**
     * Migrate given view to PostgreSql server.
     *
     * @param string $strViewName
     * @return void
     */
    private function createView(string $strViewName)
    {
        $this->sql = '';
        $boolViewErrorDirExists = true;
        try {
            $this->Log->write(PHP_EOL . "\t" . '-- Attempting to create view: "' .
                $this->dbSchema . '"."' . $strViewName . '"...' . PHP_EOL, 'common');
            $this->connect();

            $this->sql = 'SHOW CREATE VIEW `' . $strViewName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrColumns = $stmt->fetchAll(PDO::FETCH_ASSOC);
            unset($this->sql, $stmt);

            $this->sql = ViewGenerator::generateView(
                $this->dbSchema,
                $strViewName,
                $arrColumns[0]['Create View']
            );
            $stmt = $this->pgsql->query($this->sql);
            unset($this->sql, $stmt, $arrColumns);
            $this->Log->write(PHP_EOL . "\t" . '-- View: "' . $this->dbSchema .
                '"."' . $strViewName . '" is created...' . PHP_EOL);
        } catch (PDOException $exception) {
            $strMsg = $boolViewErrorDirExists && file_exists($this->viewErrorsDirPath .
                '/' . $strViewName . '.sql') ? __METHOD__ . PHP_EOL . "\t" . '-- Cannot create view "' .
                $this->dbSchema . '"."' . $strViewName . '" ' . PHP_EOL . "\t" . '-- You can find view definition at
                 "logs_directory/not_created_views/' . $strViewName . '.sql"' . PHP_EOL . "\t" . '-- You can try to 
                 fix view definition script and run it manually.' : __METHOD__ . PHP_EOL . "\t" . '-- Cannot create 
                 view "' . $this->dbSchema . '"."' . $strViewName . '" ';

            $this->Log->write(PHP_EOL . "\t" . '-- Cannot create view "' . $this->dbSchema .
                '"."' . $strViewName . '" ' . PHP_EOL);
            $this->Log->generateError($exception, $strMsg, $this->sql);
            unset($strMsg, $this->boolViewErrorsDirExists, $this->sql);
        }
    }

    /**
     * Migrate structure of a single table to PostgreSql server.
     *
     * @param string $strTableName
     * @return bool
     */
    private function createTable(string $strTableName): bool
    {
        $boolRetVal = false;
        $this->sql = '';

        try {
            $this->Log->write(PHP_EOL . '-- Currently processing table: ' . $strTableName . '...' . PHP_EOL);
            $this->connect();

            $this->sql = 'SHOW FULL COLUMNS FROM `' . $strTableName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrColumns = $stmt->fetchAll(PDO::FETCH_ASSOC);
            unset($this->sql, $stmt);

            $strSqlCreateTable = 'CREATE TABLE "' . $this->dbSchema . '"."' . $strTableName . '"(';

            foreach ($arrColumns as $arrColumn) {
                $strSqlCreateTable .= '"' . $arrColumn['Field'] . '" ' . MapDataTypes::map($arrColumn['Type']) . ',';
                unset($arrColumn);
            }

            $strSqlCreateTable = substr($strSqlCreateTable, 0, -1) . ');';
            $stmt = $this->pgsql->query($strSqlCreateTable);
            $boolRetVal = true;

            unset($strSqlCreateTable, $stmt, $arrColumns);
            $this->Log->write(
                "\t" . '-- Table "' . $this->dbSchema . '"."' . $strTableName . '" ' . 'is created.' . PHP_EOL,
                'common'
            );
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot create table "' . $this->dbSchema .
                '"."' . $strTableName . '".';
            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);
        }

        try {
            $this->sql = 'SHOW TABLE STATUS WHERE Name="' . $strTableName . '";';
            $stmt = $this->mysql->query($this->sql);
            $arrTableData = $stmt->fetch(PDO::FETCH_ASSOC);

            if (!empty($arrTableData['Comment'])) {
                $this->sql = 'COMMENT ON TABLE "' . $this->dbSchema . '"."' . $strTableName
                    . '" IS ' . $this->pgsql->quote($arrTableData['Comment']);
                $this->pgsql->query($this->sql);
            }
            unset($this->sql, $stmt);
        } catch (PDOException $e) {
            // Log the error but don't fail because a missing table comment is not critical to migration.
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Cannot add comment "' . $this->dbSchema .
                '"."' . $strTableName . '".';
            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);
        }

        return $boolRetVal;
    }

    /**
     * Escape the given string for the PostgreSQL COPY text format.
     *
     * @param string $value
     * @return string
     */
    private function escapeValue(string $value): string
    {
        return $this->Utility->escapeValue($value);
    }

    /**
     * Save a chunk of rows into PostgreSQL
     *
     * @param string $strTableName
     * @param array $copyArray
     * @return int    Number of rows inserted.
     */
    private function copySaveRows(string $strTableName, array $copyArray): int
    {
        $intRetVal = 0;
        // Attempt to copy, if it fails, do one at a time to ensure we find all the errors.
        // If the data is valid, we perform fast, otherwise we ensure correctness at the cost of speed.

        if (!@pg_copy_from($this->pgSqlData, "\"" . $this->dbSchema . "\".\"" . $strTableName . "\"", $copyArray)) {
            // do each row, logging the failed ones.
            $this->Log->write("\t-- The following contains rows rejected by PostgreSQL for table " .
                "\"" . $this->dbSchema . "\".\"" . $strTableName . "\"\n");
            foreach ($copyArray as $copyRow) {
                if (
                    !@pg_copy_from($this->pgSqlData, "\"" . $this->dbSchema . "\".\"" .
                        $strTableName . "\"", [$copyRow])
                ) {
                    $this->Log->write($copyRow);
                } else {
                    $intRetVal++;
                }
            }
            $this->Log->write("-- End of failed rows --\n");
        } else {
            $intRetVal += count($copyArray);
        }

        return $intRetVal;
    }

    /**
     * Load a chunk of data using "PostgreSql COPY".
     *
     * @param string $strTableName
     * @param string $strSelectFieldList
     * @param array $arrColumns
     * @param int $intRowsInChunk
     * @param int $intRowsCnt
     * @return int
     */
    private function populateTableData(
        string $strTableName,
        string $strSelectFieldList,
        array $arrColumns,
        int $intRowsInChunk,
        int $intRowsCnt
    ): int {
        $intRetVal = 0;
        $this->sql = '';
        $sqlCopy = '';
        $copiedCount = 0;
        $copyArray = [];

        try {
            $this->connect();
            $this->sql = 'SELECT ' . $strSelectFieldList . ' FROM `' . $strTableName . '`;';
            $stmt = $this->mysql->prepare($this->sql);
            $arrRow = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->mysql->setAttribute(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);
            //$stmt->execute();
            $arrBinaryFields = [];

            // Calculate column types at the top for performance
            foreach ($arrColumns as $column) {
                if (
                    stripos($column['Type'], 'blob') !== false
                    || stripos($column['Type'], 'binary') !== false
                ) {
                    $arrBinaryFields[$column['Field']] = true;
                }
            }

            /*
             * Ensure correctness of encoding and insert data into temporary csv file.
             */
            while ($arrRow = $stmt->fetch(PDO::FETCH_ASSOC)) {
                $boolValidCsvEntity = true;
                $arrSanitizedCsvData = [];
                $copiedCount++;

                foreach ($arrRow as $name => $value) {
                    if (is_null($value)) {
                        $arrSanitizedCsvData[] = '\N';
                    } elseif (isset($arrBinaryFields[$name])) {
                        // Binary types need \x for hex escaping and will receive hex from the MySQL query.
                        $arraySanitizedCsvData[] = '\x' . $value;
                    } elseif (mb_check_encoding($value, $this->dbEncoding)) {
                        $arrSanitizedCsvData[] = $this->escapeValue($value);
                    } else {
                        $value = mb_convert_encoding($value, $this->dbEncoding);

                        if (mb_check_encoding($value, $this->dbEncoding)) {
                            $arrSanitizedCsvData[] = $this->escapeValue($value);
                        } else {
                            $boolValidCsvEntity = false;
                        }
                    }
                }

                if ($boolValidCsvEntity) {
                    $copyArray[] = implode("\t", $arrSanitizedCsvData[]) . "\n";
                }

                if (($copiedCount % $intRowsInChunk) == 0) {
                    $intRetVal += $this->copySaveRows($strTableName, $copyArray);
                    $copiedCount = 0;
                    $copyArray = [];
                }
            }
            $intRetVal += $this->copySaveRows($strTableName, $copyArray);
            $copiedCount = 0;
            $copyArray = [];
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL;
            $this->Log->generateError($e, $strMsg, $this->sql . PHP_EOL . $sqlCopy);
            $this->Log->write("\t--Following MySQL query will return a data set, rejected by 
            PostgreSQL:\n" . $this->sql . "\n");
        }

        return $intRetVal;
    }

    /**
     * Arranges columns data before loading.
     *
     * @param array $arrColumns
     * @return string
     */
    private function arrangeColumnsData(array $arrColumns): string
    {
        $strRetVal = '';

        foreach ($arrColumns as $arrColumn) {
            if (
                stripos($arrColumn['Type'], 'geometry') !== false
                || stripos($arrColumn['Type'], 'point') !== false
                || stripos($arrColumn['Type'], 'linestring') !== false
                || stripos($arrColumn['Type'], 'polygon') !== false
            ) {
                $strRetVal .= 'HEX(ST_AsWKB(`' . $arrColumn['Field'] . '`)) AS `' . $arrColumn['Field'] . '`,';
            } elseif (
                stripos($arrColumn['Type'], 'bit') !== false
            ) {
                $strRetVal .= 'BIN(`' . $arrColumn['Field'] . '`) AS `' . $arrColumn['Field'] . '`,';
            } elseif (
                stripos($arrColumn['Type'], 'timestamp') !== false
                || stripos($arrColumn['Type'], 'date') !== false
            ) {
                $strRetVal .= 'IF(`' . $arrColumn['Field']
                    . '` IN(\'0000-00-00\', \'0000-00-00 00:00:00\'), \'-INFINITY\', `'
                    . $arrColumn['Field'] . '`) AS `' . $arrColumn['Field'] . '`,';
            } else {
                $strRetVal .= '`' . $arrColumn['Field'] . '`,';
            }
        }

        return substr($strRetVal, 0, -1);
    }

    /**
     * Populate current table.
     *
     * @param string $strTableName
     * @return array
     */
    private function populateTable(string $strTableName): array
    {
        $intRetVal = 0;
        $this->sql = '';
        $intRowsCnt = 0;
        try {
            $this->Log->write(
                "\t" . '-- Populating table "' . $this->dbSchema . '"."' . $strTableName . '" ' . PHP_EOL,
                'common'
            );
            $this->connect();

            // Determine current table size, apply "chunking".
            $this->sql = "SELECT ((data_length + index_length) / 1024 / 1024) AS size_in_mb
                    FROM information_schema.TABLES
                    WHERE table_schema = '" . $this->mySqlDbName . "'
                      AND table_name = '" . $strTableName . "';";

            $stmt = $this->mysql->query($this->sql);
            $arrRows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $floatTableSizeInMb = (float)$arrRows[0]['size_in_mb'];
            $floatTableSizeInMb = max($floatTableSizeInMb, 1);
            unset($this->sql, $stmt, $arrRows);

            $this->sql = 'SELECT COUNT(1) AS rows_count FROM `' . $strTableName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrRows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $intRowsCnt = (int)$arrRows[0]['rows_count'];
            $floatChunksCnt = $floatTableSizeInMb / $this->dataChunkSize;
            $floatChunksCnt = max($floatChunksCnt, 1);
            $intRowsInChunk = ceil($intRowsCnt / $floatChunksCnt);
            unset($this->sql, $stmt, $arrRows);

            // Build field list for SELECT from MySQL and apply optional casting or function based on field type.
            $this->sql = 'SHOW FULL COLUMNS FROM `' . $strTableName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrColumns = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $strSelectFieldList = $this->arrangeColumnsData($arrColumns);
            unset($this->sql, $stmt);
            // End field list for SELECT from MySQL.

            $this->Log->write(
                "\t" . '-- Total rows to insert into "' . $this->dbSchema . '"."'
                . $strTableName . '": ' . $intRowsCnt . PHP_EOL,
                'common'
            );

            $intRetVal = $this->populateTableData(
                $strTableName,
                $strSelectFieldList,
                $arrColumns,
                $intRowsInChunk,
                $intRowsCnt
            );
            $this->Log->write(
                "\t" . '-- Total rows inserted into "' . $this->dbSchema . '"."'
                . $strTableName . '": ' . $intRetVal . PHP_EOL,
                'common'
            );
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL;
            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);
        }

        echo PHP_EOL, PHP_EOL;

        return [$intRowsCnt, $intRowsCnt - $intRetVal];
    }

    /**
     * Define which columns of the given table can contain the "NULL" value.
     * Set an appropriate constraint, if needed.
     *
     * @param string $strTableName
     * @param array $arrColumns
     * @return void
     */
    private function processNull(string $strTableName, array $arrColumns)
    {
        $this->sql = '';

        $this->Log->write(
            PHP_EOL . "\t" . '-- Define "NULLs" for table: "' . $this->dbSchema .
            '"."' . $strTableName . '"...' . PHP_EOL,
            'common'
        );

        foreach ($arrColumns as $arrColumn) {
            try {
                $this->connect();

                if ('no' == strtolower($arrColumn['Null'])) {
                    $this->sql = 'ALTER TABLE "' . $this->dbSchema . '"."' . $strTableName
                        . '" ALTER COLUMN "' . $arrColumn['Field'] . '" SET NOT NULL;';

                    $stmt = $this->pgsql->query($this->sql);
                    unset($this->sql, $stmt);
                }
            } catch (PDOException $e) {
                $this->Log->generateError($e, __METHOD__ . PHP_EOL, $this->sql);
            }

            unset($arrColumn);
        }

        $this->Log->write("\t-- Done." . PHP_EOL, 'common');
    }

    /**
     * Create comments.
     *
     * @param string $strTableName
     * @param array $arrColumns
     * @return void
     */
    private function processComment(string $strTableName, array $arrColumns)
    {
        $this->sql = '';

        foreach ($arrColumns as $arrColumn) {
            if (!isset($arrColumn['Comment'])) {
                continue;
            }

            try {
                $this->connect();
                $this->sql = 'COMMENT ON COLUMN "' . $this->dbSchema . '"."' . $strTableName . '"."'
                    . $arrColumn['Field'] . '" IS ' . $this->pgsql->quote($arrColumn['Comment']);

                $stmt = $this->pgsql->query($this->sql);

                if ($stmt === false) {
                    $this->Log->write(
                        "\t" . '-- Cannot create comment on column "' . $arrColumn['Field'] . '"...' . PHP_EOL,
                        'common'
                    );
                } else {
                    $this->Log->write(
                        "\t" . '-- Comment on column "' . $arrColumn['Field'] . '" has set...' . PHP_EOL,
                        'common'
                    );
                }
            } catch (PDOException $e) {
                $this->Log->generateError($e, __METHOD__ . PHP_EOL, $this->sql);
            }
        }
    }

    /**
     * Define which columns of the given table have default value.
     * Set default values, if needed.
     *
     * @param string $strTableName
     * @param array $arrColumns
     * @return void
     */
    private function processDefault(string $strTableName, array $arrColumns)
    {
        $this->Log->write(
            PHP_EOL . "\t" . '-- Set default values for table: "'
            . $this->dbSchema . '"."' . $strTableName . '"...' . PHP_EOL,
            'common'
        );

        $this->sql = '';
        $arrSqlReservedValues = [
            'CURRENT_DATE' => 'CURRENT_DATE',
            '0000-00-00' => "'-INFINITY'",
            'CURRENT_TIME' => 'CURRENT_TIME',
            '00:00:00' => '00:00:00',
            'CURRENT_TIMESTAMP' => 'CURRENT_TIMESTAMP',
            '0000-00-00 00:00:00' => "'-INFINITY'",
            'LOCALTIME' => 'LOCALTIME',
            'LOCALTIMESTAMP' => 'LOCALTIMESTAMP',
            'NULL' => 'NULL',
            'UTC_DATE' => "(CURRENT_DATE AT TIME ZONE 'UTC')",
            'UTC_TIME' => "(CURRENT_TIME AT TIME ZONE 'UTC')",
            'UTC_TIMESTAMP' => "(NOW() AT TIME ZONE 'UTC')",
        ];

        foreach ($arrColumns as $arrColumn) {
            if (!isset($arrColumn['Default'])) {
                $this->Log->write(
                    "\t" . '-- Default value for column "' . $arrColumn['Field'] .
                    '" has not been detected...' . PHP_EOL,
                    'common'
                );
                continue;
            }

            try {
                $this->connect();
                $this->sql = 'ALTER TABLE "' . $this->dbSchema . '"."' . $strTableName . '" '
                    . 'ALTER COLUMN "' . $arrColumn['Field'] . '" SET DEFAULT ';

                if (isset($arrSqlReservedValues[$arrColumn['Default']])) {
                    $this->sql .= $arrSqlReservedValues[$arrColumn['Default']] . ';';
                } elseif (substr($arrColumn['Type'], 0, 3) === 'bit' && substr($arrColumn['Default'], 0, 2) === "b'") {
                    // This is a default for a bit column use PostgreSql syntax.
                    $this->sql .= substr($arrColumn['Default'], 1) . "::bit;";
                } else {
                    $this->sql .= is_numeric($arrColumn['Default'])
                        ? $arrColumn['Default'] . ';'
                        : " '" . $arrColumn['Default'] . "';";
                }

                $stmt = $this->pgsql->query($this->sql);

                if ($stmt === false) {
                    $this->Log->write(
                        "\t" . '-- Cannot define the default value for column "' .
                        $arrColumn['Field'] . '"...' . PHP_EOL,
                        'common'
                    );
                } else {
                    $this->Log->write(
                        "\t" . '-- The default value for column "' . $arrColumn['Field'] .
                        '" has defined...' . PHP_EOL,
                        'common'
                    );
                }
            } catch (PDOException $e) {
                $this->Log->generateError($e, __METHOD__ . PHP_EOL, $this->sql);
            }

            unset($arrColumn, $this->sql, $stmt);
        }

        unset($arrSqlReservedValues);
    }

    /**
     * Define which columns of the given table are of type "enum".
     * Set an appropriate constraint, if needed.
     *
     * @param string $strTableName
     * @param array $arrColumns
     * @return void
     */
    private function processEnum(string $strTableName, array $arrColumns)
    {
        $this->Log->write(PHP_EOL . "\t" . '-- Set "ENUMs" for table "' . $this->dbSchema .
            '"."' . $strTableName . '"...' . PHP_EOL, 'common');
        $this->sql = '';

        foreach ($arrColumns as $arrColumn) {
            try {
                $this->connect();
                $parenthesesFirstOccurrence = strpos($arrColumn['Type'], '(');

                if (false !== $parenthesesFirstOccurrence) {
                    $arrType = explode('(', $arrColumn['Type']);

                    if ('enum' == $arrType[0]) {
                        // $arrType[1] ends with ')'.
                        $this->sql = 'ALTER TABLE "' . $this->dbSchema . '"."' . $strTableName . '" '
                            . 'ADD CHECK ("' . $arrColumn['Field'] . '" IN (' . $arrType[1] . ');';

                        $stmt = $this->pgsql->query($this->sql);

                        if (false === $stmt) {
                            $this->Log->write(
                                "\t" . '-- Cannot set "ENUM" for column "' . $arrColumn['Field'] . '"'
                                . PHP_EOL . '...Column "' . $arrColumn['Field']
                                . '" has defined as "CHARACTER VARYING(255)"...' . PHP_EOL,
                                'common'
                            );
                        } else {
                            $this->Log->write(
                                "\t" . '-- "CHECK" was successfully added to column "' .
                                $arrColumn['Field'] . '"...' . PHP_EOL,
                                'common'
                            );
                        }

                        unset($this->sql, $stmt);
                    }

                    unset($arrType);
                }
            } catch (PDOException $e) {
                $this->Log->generateError($e, __METHOD__ . PHP_EOL, $this->sql);
            }

            unset($arrColumn, $parenthesesFirstOccurrence);
        }
    }

    /**
     * Define which column in given table has the "auto_increment" attribute.
     * Create an appropriate sequence.
     *
     * @param string $strTableName
     * @param array $arrColumns
     * @return void
     */
    private function createSequence(string $strTableName, array $arrColumns)
    {
        $this->sql = '';
        $strSeqName = '';
        $boolSequenceCreated = false;

        try {
            $this->connect();

            foreach ($arrColumns as $arrColumn) {
                if ('auto_increment' == $arrColumn['Extra']) {
                    $strSeqName = $strTableName . '_' . $arrColumn['Field'] . '_seq';
                    $this->Log->write("\t" . '-- Trying to create sequence "' . $this->dbSchema .
                        '"."' . $strSeqName . '"...' . PHP_EOL);
                    $this->sql = 'CREATE SEQUENCE "' . $this->dbSchema . '"."' . $strSeqName . '";';
                    $stmt = $this->pgsql->query($this->sql);

                    if (false === $stmt) {
                        $this->Log->write("\t" . '-- Failed to create sequence "' . $this->dbSchema .
                            '"."' . $strSeqName . '"...' . PHP_EOL);
                        unset($stmt, $this->sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $this->sql);
                    }

                    $this->sql = sprintf(
                        "ALTER TABLE \"%s\".\"%s\" ALTER COLUMN \"%s\" SET DEFAULT 
                        NEXTVAL('\"%s\".\"%s\"');",
                        $this->dbSchema,
                        $strTableName,
                        $arrColumn['Field'],
                        $this->dbSchema,
                        $strSeqName
                    );

                    $stmt = $this->pgsql->query($this->sql);

                    if (false === $stmt) {
                        $this->Log->write(
                            "\t" . '-- Failed to set default value for "' . $this->dbSchema . '"."'
                            . $strTableName . '"."' . $arrColumn['Field'] . '"...' . PHP_EOL
                            . "\t" . '-- Note: sequence "' . $this->dbSchema . '"."'
                            . $strSeqName . '" was created...' . PHP_EOL
                        );

                        unset($stmt, $this->sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $this->sql);
                    }

                    $this->sql = 'ALTER SEQUENCE "' . $this->dbSchema . '"."' . $strSeqName . '" '
                        . 'OWNED BY "' . $this->dbSchema . '"."' . $strTableName . '"."' . $arrColumn['Field'] . '";';

                    $stmt = $this->pgsql->query($this->sql);

                    if (false === $stmt) {
                        $this->Log->write(
                            "\t" . '-- Failed to relate sequence "' . $this->dbSchema . '"."'
                            . $strSeqName . '" to "' . $this->dbSchema . '"."' . $strTableName . '"'
                            . PHP_EOL . "\t" . '-- Note: sequence "' . $this->dbSchema . '"."'
                            . $strSeqName . '" was created...' . PHP_EOL
                        );

                        unset($stmt, $this->sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $this->sql);
                    }

                    $this->sql = 'SELECT SETVAL(\'"' . $this->dbSchema . '"."' . $strSeqName . '"\', '
                        . '(SELECT MAX("' . $arrColumn['Field'] . '") FROM "'
                        . $this->dbSchema . '"."' . $strTableName . '"));';

                    $stmt = $this->pgsql->query($this->sql);

                    if (false === $stmt) {
                        $this->Log->write(
                            "\t" . '-- Failed to set max-value of "' . $this->dbSchema . '"."' . $strTableName
                            . '"."' . $arrColumn['Field'] . '" as the "NEXTVAL of "' . $this->dbSchema
                            . '."' . $strSeqName . '"...' . PHP_EOL
                            . "\t" . '-- Note: sequence "' . $this->dbSchema . '"."'
                            . $strSeqName . '" was created...' . PHP_EOL
                        );

                        unset($stmt, $this->sql, $arrColumn);
                        break;
                    } else {
                        unset($stmt, $this->sql);
                    }

                    $boolSequenceCreated = true;
                }

                if ($boolSequenceCreated) {
                    unset($arrColumn);
                    $this->Log->write("\t" . '-- Sequence "' . $this->dbSchema . '"."' .
                        $strSeqName . '" was created...' . PHP_EOL);
                    break;
                }
                unset($arrColumn);
            }
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Failed to create sequence "' . $this->dbSchema
                . '"."' . $strSeqName . '"...' . PHP_EOL;

            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);
        }
    }

    /**
     * Create primary key and indices.
     *
     * @param string $strTableName
     * @param array $arrColumns
     * @return void
     */
    private function processIndexAndKey(string $strTableName, array $arrColumns)
    {
        $this->sql = '';

        try {
            $this->Log->write(
                "\t" . '-- Set primary key and indices for table "' . $this->dbSchema
                . '"."' . $strTableName . '"...' . PHP_EOL,
                'common'
            );

            $this->connect();
            $this->sql = 'SHOW INDEX FROM `' . $strTableName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrIndices = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $arrPgIndices = [];
            $intCounter = 0;
            $strCurrentAction = '';
            unset($this->sql, $stmt);

            foreach ($arrIndices as $arrIndex) {
                if (isset($arrPgIndices[$arrIndex['Key_name']])) {
                    $arrPgIndices[$arrIndex['Key_name']]['column_name'][] = '"' . $arrIndex['Column_name'] . '"';
                } else {
                    $arrPgIndices[$arrIndex['Key_name']] = [
                        'is_unique' => 0 == $arrIndex['Non_unique'],
                        'column_name' => ['"' . $arrIndex['Column_name'] . '"'],
                        'Index_type' => ' USING ' . ($arrIndex['Index_type'] === 'SPATIAL' ?
                                'GIST' : $arrIndex['Index_type']),
                    ];
                }
                unset($arrIndex);
            }

            unset($arrIndices);

            foreach ($arrPgIndices as $strKeyName => $arrIndex) {
                $this->sql = '';

                if (strtolower($strKeyName) === 'primary') {
                    $strCurrentAction = 'PK';
                    $this->sql = 'ALTER TABLE "' . $this->dbSchema . '"."' . $strTableName . '" '
                        . 'ADD PRIMARY KEY(' . implode(',', $arrIndex['column_name']) . ');';
                } elseif ($arrIndex['is_unique']) {
                    // "schema_idxname_{integer}_idx" - is NOT a mistake.
                    $strColumnName = str_replace('"', '', $arrIndex['column_name'][0]) . $intCounter;
                    $strIndexName = $this->dbSchema . '_' . $strTableName . '_' . $strColumnName . '_idx';
                    $strColumnList = '(' . implode(',', $arrIndex['column_name']) . ')';
                    $strCurrentAction = 'uniqueindex';
                    $this->sql = "ALTER TABLE \"" . $this->dbSchema . "\".\"" . $strTableName . "\" 
                    ADD CONSTRAINT \"" . $strIndexName . "\" UNIQUE " . $strColumnList . ";";
                } else {
                    // "schema_idxname_{integer}_idx" - is NOT a mistake.
                    $strColumnName = str_replace('"', '', $arrIndex['column_name'][0]) . $intCounter;
                    $strCurrentAction = 'index';
                    $this->sql = 'CREATE INDEX "'
                        . $this->dbSchema . '_' . $strTableName . '_' . $strColumnName . '_idx" ON "'
                        . $this->dbSchema . '"."' . $strTableName . '" ' . $arrIndex['Index_type']
                        . ' (' . implode(',', $arrIndex['column_name']) . ');';

                    unset($strColumnName);
                }

                $stmt = $this->pgsql->query($this->sql);

                if (false === $stmt) {
                    $this->Log->write(
                        "\t" . '-- Failed to set ' . $strCurrentAction . ' for table "'
                        . $this->dbSchema . '"."' . $strTableName . '"...' . PHP_EOL,
                        'common'
                    );
                } else {
                    $this->Log->write(
                        "\t-- " . $strCurrentAction . ' for table "'
                        . $this->dbSchema . '"."' . $strTableName . '" are set...' . PHP_EOL,
                        'common'
                    );
                }

                unset($this->sql, $stmt, $strKeyName, $arrIndex);
                $intCounter++;
            }

            unset($arrPgIndices, $intCounter);
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t"
                . '-- Error occurred when tried to set primary key and indices for table "'
                . $this->dbSchema . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);
        }
    }

    /**
     * Create foreign keys.
     *
     * @param string $strTableName
     * @return void
     */
    private function processForeignKey(string $strTableName)
    {
        $this->sql = '';

        try {
            $this->Log->write("\t" . '-- Search foreign key for table "' . $this->dbSchema .
                '"."' . $strTableName . '"...' . PHP_EOL, 'common');
            $this->connect();

            $this->sql = "SELECT cols.COLUMN_NAME,
                           refs.REFERENCED_TABLE_NAME,
                           refs.REFERENCED_COLUMN_NAME,
                           cRefs.UPDATE_RULE,
                           cRefs.DELETE_RULE,
                           cRefs.CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.`COLUMNS` AS cols
                    INNER JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS refs
                    ON refs.TABLE_SCHEMA = cols.TABLE_SCHEMA
                        AND refs.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA
                        AND refs.TABLE_NAME = cols.TABLE_NAME
                        AND refs.COLUMN_NAME = cols.COLUMN_NAME
                    LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cRefs
                    ON cRefs.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA
                        AND cRefs.CONSTRAINT_NAME = refs.CONSTRAINT_NAME
                    LEFT JOIN INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` AS links
                    ON links.TABLE_SCHEMA = cols.TABLE_SCHEMA
                        AND links.REFERENCED_TABLE_SCHEMA = cols.TABLE_SCHEMA
                        AND links.REFERENCED_TABLE_NAME = cols.TABLE_NAME
                        AND links.REFERENCED_COLUMN_NAME = cols.COLUMN_NAME
                    LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS cLinks
                    ON cLinks.CONSTRAINT_SCHEMA = cols.TABLE_SCHEMA
                        AND cLinks.CONSTRAINT_NAME = links.CONSTRAINT_NAME
                    WHERE cols.TABLE_SCHEMA = '" . $this->mySqlDbName . "' AND cols.TABLE_NAME = '" . $strTableName
                . "';";

            $stmt = $this->mysql->query($this->sql);
            $arrForeignKeys = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $arrConstraints = [];
            unset($this->sql, $stmt);

            foreach ($arrForeignKeys as $arrFk) {
                $arrConstraints[$arrFk['CONSTRAINT_NAME']][] = [
                    'column_name' => $arrFk['COLUMN_NAME'],
                    'referenced_table_name' => $arrFk['REFERENCED_TABLE_NAME'],
                    'referenced_column_name' => $arrFk['REFERENCED_COLUMN_NAME'],
                    'update_rule' => $arrFk['UPDATE_RULE'],
                    'delete_rule' => $arrFk['DELETE_RULE'],
                ];

                unset($arrFk);
            }

            unset($arrForeignKeys);

            foreach ($arrConstraints as $arrRows) {
                $arrFKs = [];
                $arrPKs = [];
                $strRefTbName = '';
                $strDeleteRule = '';
                $strUpdateRule = '';
                $this->sql = 'ALTER TABLE "' . $this->dbSchema . '"."' . $strTableName . '" ADD FOREIGN KEY (';

                foreach ($arrRows as $arrRow) {
                    $strRefTbName = $arrRow['referenced_table_name'];
                    $strUpdateRule = $arrRow['update_rule'];
                    $strDeleteRule = $arrRow['delete_rule'];
                    $arrFKs[] = '"' . $arrRow['column_name'] . '"';
                    $arrPKs[] = '"' . $arrRow['referenced_column_name'] . '"';
                    unset($arrRow);
                }

                $this->sql .= implode(',', $arrFKs) . ') REFERENCES "' . $this->dbSchema .
                    '"."' . $strRefTbName . '" ('
                    . implode(',', $arrPKs) . ') ON UPDATE ' . $strUpdateRule . ' ON DELETE ' . $strDeleteRule . ';';

                $stmt = $this->pgsql->query($this->sql);

                if (false === $stmt) {
                    $this->Log->write(
                        "\t" . '-- Failed to set foreign keys for table "'
                        . $this->dbSchema . '"."' . $strTableName . '"...' . PHP_EOL,
                        'common'
                    );
                } else {
                    $this->Log->write(
                        "\t" . '-- Foreign key for table "'
                        . $this->dbSchema . '"."' . $strTableName . '" is set...' . PHP_EOL,
                        'common'
                    );
                }

                unset($this->sql, $stmt, $arrFKs, $arrPKs, $strRefTbName, $strDeleteRule, $strUpdateRule, $arrRows);
            }
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t"
                . '-- Error occurred when tried to create foreign key for table "'
                . $this->dbSchema . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);
        }
    }

    /**
     * Set constraints (excluding foreign key constraints) for given table.
     *
     * @param string $strTableName
     * @return void
     */
    private function setTableConstraints(string $strTableName): void
    {
        $this->Log->write(
            "\t" . '-- Trying to set table constraints for "' . $this->dbSchema .
            '"."' . $strTableName . '"...' . PHP_EOL,
            'common'
        );
        $arrColumns = [];
        $this->sql = '';

        try {
            $this->connect();
            $this->sql = 'SHOW FULL COLUMNS FROM `' . $strTableName . '`;';
            $stmt = $this->mysql->query($this->sql);
            $arrColumns = $stmt->fetchAll(PDO::FETCH_ASSOC);
            unset($this->sql, $stmt);
        } catch (PDOException $e) {
            $strMsg = __METHOD__ . PHP_EOL . "\t" . '-- Failed to set constraints for "' . $this->dbSchema
                . '"."' . $strTableName . '"...' . PHP_EOL;

            $this->Log->generateError($e, $strMsg, $this->sql);
            unset($strMsg);

            return;
        }

        $this->processEnum($strTableName, $arrColumns);
        $this->processNull($strTableName, $arrColumns);
        $this->processDefault($strTableName, $arrColumns);
        $this->createSequence($strTableName, $arrColumns);
        $this->processIndexAndKey($strTableName, $arrColumns);
        $this->processComment($strTableName, $arrColumns);
        $this->Log->write(
            "\t" . '-- Constraints for "' . $this->dbSchema . '"."' . $strTableName
            . '" were set successfully...' . PHP_EOL,
            'common'
        );
    }

    /**
     * Generates a summary report.
     *
     * @return string
     * @TODO - move to Log class
     */
    private function generateReport(): string
    {
        $strRetVal = PHP_EOL;
        $intLargestTableTitle = 0;
        $intLargestRecordsTitle = 0;
        $intLargestFailedTitle = 0;
        $intLargestTimeTitle = 0;

        array_unshift($this->summaryReport, ['TABLE', 'RECORDS', 'FAILED', 'DATA LOAD TIME']);

        foreach ($this->summaryReport as $arrReport) {
            $intTableTitleLength = strlen($arrReport[0]);
            $intRecordsTitleLength = strlen($arrReport[1]);
            $intFailedTitleLength = strlen($arrReport[2]);
            $intTimeTitleLength = strlen($arrReport[3]);
            $intLargestTableTitle = max($intLargestTableTitle, $intTableTitleLength);
            $intLargestRecordsTitle = max($intLargestRecordsTitle, $intRecordsTitleLength);
            $intLargestFailedTitle = max($intLargestFailedTitle, $intFailedTitleLength);
            $intLargestTimeTitle = max($intLargestTimeTitle, $intTimeTitleLength);
        }

        foreach ($this->summaryReport as $arrReport) {
            $intSpace = $intLargestTableTitle - strlen($arrReport[0]);
            $strRetVal .= "\t|  " . $arrReport[0];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |  ';

            $intSpace = $intLargestRecordsTitle - strlen($arrReport[1]);
            $strRetVal .= $arrReport[1];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |  ';

            $intSpace = $intLargestFailedTitle - strlen($arrReport[2]);
            $strRetVal .= $arrReport[2];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |  ';

            $intSpace = $intLargestTimeTitle - strlen($arrReport[3]);
            $strRetVal .= $arrReport[3];

            $strRetVal .= str_repeat(' ', $intSpace);
            $strRetVal .= '  |' . PHP_EOL . "\t";
            $intSpace = $intLargestTableTitle + $intLargestRecordsTitle + $intLargestFailedTitle +
                $intLargestTimeTitle + 21;

            $strRetVal .= str_repeat('-', $intSpace);

            $strRetVal .= PHP_EOL;
            unset($arrReport, $intSpace);
        }

        unset($intLargestTableTitle, $intLargestRecordsTitle, $intLargestTimeTitle);

        if (!empty($this->strWriteSummaryReportTo)) {
            file_put_contents($this->strWriteSummaryReportTo, $strRetVal);
        }

        return $strRetVal;
    }

    /**
     * Create tables with the basic structure (column names and data types).
     * Populate tables.
     *
     * @return bool
     */
    private function createAndPopulateTables(): bool
    {
        foreach ($this->tablesToMigrate as $arrTable) {
            $floatStartCopy = microtime(true);
            $intRecords = 0;

            if (
                !$this->isDataOnly
                && !$this->createTable($arrTable['Tables_in_' . $this->mySqlDbName])
            ) {
                return false;
            } else {
                [$intRecords, $failedRecords] = $this->populateTable($arrTable['Tables_in_' . $this->mySqlDbName]);
            }

            $floatEndCopy = microtime(true);
            $this->summaryReport[] = [
                $this->dbSchema . '.' . $arrTable['Tables_in_' . $this->mySqlDbName],
                $intRecords,
                $failedRecords,
                round(($floatEndCopy - $floatStartCopy), 3) . ' seconds',
            ];

            unset($arrTable, $floatStartCopy, $floatEndCopy, $intRecords);
        }

        return true;
    }

    /**
     * Set table constraints.
     */
    private function createConstraints()
    {
        foreach ($this->tablesToMigrate as $arrTable) {
            $this->setTableConstraints($arrTable['Tables_in_' . $this->mySqlDbName]);
            unset($arrTable);
        }
    }

    /**
     * Set foreign key constraints.
     */
    private function createForeignKeys()
    {
        foreach ($this->tablesToMigrate as $arrTable) {
            $this->processForeignKey($arrTable['Tables_in_' . $this->mySqlDbName]);
            unset($arrTable);
        }
    }

    /**
     * Attempt to create views.
     */
    private function createViews()
    {
        foreach ($this->viewsToMigrate as $arrView) {
            $this->createView($arrView['Tables_in_' . $this->mySqlDbName]);
            unset($arrView);
        }
    }

    /**
     * Performs migration from source database to destination database.
     *
     * @return void
     */
    public function migrate()
    {
        $intTimeBegin = time();
        $this->Log->write(
            PHP_EOL . "\t" . '"FromMySqlToPostgreSql" - the database migration tool' .
            PHP_EOL . "\tCopyright 2015  Anatoly Khaytovich <anatolyuss@gmail.com>" .
            PHP_EOL . "\t-- Migration began..." .
            ($this->isDataOnly ? PHP_EOL . "\t-- Only data will migrate." : '') .
            PHP_EOL,
            'common'
        );

        ini_set('memory_limit', '-1');
        $groundsForExit = false;
        /*
         * Create a database schema.
         */
        if (!$this->createSchema()) {
            $this->Log->write('-- Script is terminated.' . PHP_EOL, 'common');
            $groundsForExit = true;
        } else {
            $this->Log->write(
                '-- New schema "' . $this->dbSchema . '" was created...' . PHP_EOL,
                'common'
            );
        }

        if (!$this->loadStructureToMigrate()) {
            $this->Log->write('-- Script is terminated.' . PHP_EOL);
            $groundsForExit = true;
        } else {
            $intTablesCnt = count($this->tablesToMigrate);
            $this->Log->write(
                '-- ' . $intTablesCnt . ($intTablesCnt === 1 ? ' table ' : ' tables ') . 'detected' . PHP_EOL,
                'common'
            );
        }

        if (!$this->createAndPopulateTables()) {
            $this->Log->write('-- Script is terminated.' . PHP_EOL, 'common');
            $groundsForExit = true;
        }
        if ($groundsForExit) {
            return;
        }
        if (!$this->isDataOnly) {
            $this->createConstraints();
            $this->createForeignKeys();
            $this->createViews();
        }

        /*
         * Remove the temporary directory.
         */
        if (!rmdir($this->tempDirectory)) {
            $this->Log->write(
                '-- NOTE: directory "' . $this->tempDirectory . '" was not removed!' . PHP_EOL,
                'common'
            );
        }

        $intTimeEnd = time();
        $intExecTime = $intTimeEnd - $intTimeBegin;
        $intHours = floor($intExecTime / 3600);
        $intMinutes = ($intExecTime / 60) % 60;
        $intSeconds = $intExecTime % 60;

        $this->Log->write(
            $this->generateReport() . PHP_EOL
            . '-- Migration was successfully accomplished!' . PHP_EOL
            . '-- Total time: ' . ($intHours < 10 ? '0' . $intHours : $intHours)
            . ':' . ($intMinutes < 10 ? '0' . $intMinutes : $intMinutes)
            . ':' . ($intSeconds < 10 ? '0' . $intSeconds : $intSeconds)
            . ' (hours:minutes:seconds)' . PHP_EOL . PHP_EOL,
            'common'
        );
    }

    /**
     * @param array $arrConfig
     * @return void
     */
    public function setDefaults(array $arrConfig): void
    {
        $this->tablesToMigrate = [];
        $this->viewsToMigrate = [];
        $this->summaryReport = [];
        $this->tempDirectory = $arrConfig['temp_dir_path'];
        $this->dbEncoding = $arrConfig['config']['encoding'] ?? 'UTF-8';
        $this->dataChunkSize = isset($arrConfig['config']['data_chunk_size']) ?
            (float)$arrConfig['config']['data_chunk_size'] : 10;
        $this->dataChunkSize = max($this->dataChunkSize, 1);
        $this->sourceConString = $arrConfig['config']['source'];
        $this->targetConString = $arrConfig['config']['target'];
        $this->mySqlDbName = Utilities::extractDbName($this->sourceConString);
        $this->dbSchema = $arrConfig['config']['schema'] ?? '';
        $this->isDataOnly = isset($arrConfig['config']['data_only']) &&
            $arrConfig['config']['data_only'];
    }

    /**
     * @return void
     */
    public function createDefaultAssets(): void
    {
        if (!file_exists($this->tempDirectory)) {
            try {
                mkdir($this->tempDirectory);
            } catch (Exception $exception) {
                echo PHP_EOL,
                '-- Cannot perform a migration due to impossibility to create "temporary_directory": ',
                $this->tempDirectory,
                PHP_EOL;

                return;
            }
        }
    }
}
