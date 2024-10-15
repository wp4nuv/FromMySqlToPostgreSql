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
}
