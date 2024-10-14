<?php

namespace App;

use PDOException;

class Utilities
{
    private FromMySqlToPostgreSql $fromMySqlToPostgreSql;

    public function __construct(FromMySqlToPostgreSql $fromMySqlToPostgreSql)
    {
        $this->fromMySqlToPostgreSql = $fromMySqlToPostgreSql;
    }

    /**
     * Extract database name from given query-string.
     *
     * @param string $strConString
     * @return string
     */
    public static function extractDbName(string $strConString): string
    {
        $strRetVal = '';
        $arrParams = explode(',', $strConString, 3);
        $arrParams2 = explode(';', $arrParams[0]);

        foreach ($arrParams2 as $strPair) {
            $arrPair = explode('=', $strPair);

            if ('dbname' == $arrPair[0]) {
                $strRetVal = $arrPair[1];
                unset($strPair);
                break;
            }
            unset($strPair);
        }
        unset($arrParams, $arrParams2);

        return $strRetVal;
    }

    /**
     * Writes a detailed error message to the log file, if specified.
     *
     * @param \PDOException $exception
     * @param string $strMessage
     * @param string $strSql
     * @return void
     */
    public function generateError(PDOException $exception, string $strMessage, string $strSql = '')
    {
        $strError = PHP_EOL . "\t-- " . $strMessage . PHP_EOL
            . "\t-- PDOException code: " . $exception->getCode() . PHP_EOL
            . "\t-- File: " . $exception->getFile() . PHP_EOL
            . "\t-- Line: " . $exception->getLine() . PHP_EOL
            . "\t-- Message: " . $exception->getMessage()
            . (empty($strSql) ? '' : PHP_EOL . "\t-- SQL: " . $strSql . PHP_EOL)
            . PHP_EOL
            . "\t-------------------------------------------------------"
            . PHP_EOL . PHP_EOL;

        $this->log($strError, true);

        if (!empty($this->fromMySqlToPostgreSql->getStrWriteErrorLogTo())) {
            if (is_resource($this->fromMySqlToPostgreSql->getResourceErrorLog())) {
                fwrite($this->fromMySqlToPostgreSql->getResourceErrorLog(), $strError);
            } else {
                $this->fromMySqlToPostgreSql->setResourceErrorLog(
                    fopen($this->fromMySqlToPostgreSql->getStrWriteErrorLogTo(), 'a')
                );

                if (is_resource($this->fromMySqlToPostgreSql->getResourceErrorLog())) {
                    fwrite($this->fromMySqlToPostgreSql->getResourceErrorLog(), $strError);
                }
            }
        }
        unset($strError);
    }

    /**
     * Escape the given string for the PostgreSQL COPY text format.
     *
     * @param string $value
     * @return string
     */
    public function escapeValue(string $value): string
    {
        return str_replace(
            ["\\", "\n", "\r", "\t"],
            ["\\\\", "\\n", "\\r", "\\t"],
            $value
        );
    }

    /**
     * Outputs given log.
     * Writes given string to the "common logs" file.
     *
     * @param string $strLog
     * @param bool $boolIsError
     * @return void
     */
    public function log(string $strLog, bool $boolIsError = false)
    {
        if (!$boolIsError) {
            echo $strLog;
        }

        if (!empty($this->fromMySqlToPostgreSql->getStrWriteCommonLogTo())) {
            if (is_resource($this->fromMySqlToPostgreSql->getResourceCommonLog())) {
                fwrite($this->fromMySqlToPostgreSql->getResourceCommonLog(), $strLog);
            } else {
                $this->fromMySqlToPostgreSql->setResourceCommonLog(
                    fopen($this->fromMySqlToPostgreSql->getStrWriteCommonLogTo(), 'a')
                );

                if (is_resource($this->fromMySqlToPostgreSql->getResourceCommonLog())) {
                    fwrite($this->fromMySqlToPostgreSql->getResourceCommonLog(), $strLog);
                }
            }
        }
    }
}
