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

/**
 * This class translates mysql data types into postgresql data types.
 *
 * @author Anatoly Khaytovich
 */
class MapDataTypes
{
    /**
     * The purpose of explicit private constructor is
     * to prevent an instance initialization.
     *
     */
    private function __construct()
    {
        // No code should be put here.
    }

    /**
     * Dictionary of MySql data types with corresponding PostgreSql data types.
     *
     * @var array
     */
    private static array $mySqlPgSqlTypesMap = [
        'json' => [
            'increased_size' => '',
            'type' => 'json',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'bit' => [
            'increased_size' => 'bit varying',
            'type' => 'bit varying',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'year' => [
            'increased_size' => 'int',
            'type' => 'smallint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'tinyint' => [
            'increased_size' => 'int',
            'type' => 'smallint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'smallint' => [
            'increased_size' => 'int',
            'type' => 'smallint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'mediumint' => [
            'increased_size' => 'bigint',
            'type' => 'int',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'int' => [
            'increased_size' => 'bigint',
            'type' => 'int',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'bigint' => [
            'increased_size' => 'bigint',
            'type' => 'bigint',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'float' => [
            'increased_size' => 'double precision',
            'type' => 'real',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'double' => [
            'increased_size' => 'double precision',
            'type' => 'double precision',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'double precision' => [
            'increased_size' => 'double precision',
            'type' => 'double precision',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'numeric' => [
            'increased_size' => '',
            'type' => 'numeric',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'decimal' => [
            'increased_size' => '',
            'type' => 'decimal',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'decimal(19,2)' => [
            'increased_size' => 'numeric',
            'type' => 'money',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'char' => [
            'increased_size' => '',
            'type' => 'character',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'varchar' => [
            'increased_size' => '',
            'type' => 'character varying',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'date' => [
            'increased_size' => '',
            'type' => 'date',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'time' => [
            'increased_size' => '',
            'type' => 'time',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'datetime' => [
            'increased_size' => '',
            'type' => 'timestamp',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'timestamp' => [
            'increased_size' => '',
            'type' => 'timestamp',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'geometry' => [
            'increased_size' => '',
            'type' => 'geometry',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'point' => [
            'increased_size' => '',
            'type' => 'point',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'linestring' => [
            'increased_size' => '',
            'type' => 'line',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'polygon' => [
            'increased_size' => '',
            'type' => 'polygon',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'enum' => [
            'increased_size' => '',
            'type' => 'character varying(255)',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'set' => [
            'increased_size' => '',
            'type' => 'character varying(255)',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'tinytext' => [
            'increased_size' => '',
            'type' => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'mediumtext' => [
            'increased_size' => '',
            'type' => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'longtext' => [
            'increased_size' => '',
            'type' => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'text' => [
            'increased_size' => '',
            'type' => 'text',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'varbinary' => [
            'increased_size' => '',
            'type' => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'binary' => [
            'increased_size' => '',
            'type' => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => true,
        ],

        'tinyblob' => [
            'increased_size' => '',
            'type' => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'mediumblob' => [
            'increased_size' => '',
            'type' => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'longblob' => [
            'increased_size' => '',
            'type' => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],

        'blob' => [
            'increased_size' => '',
            'type' => 'bytea',
            'mySqlVarLenPgSqlFixedLen' => false,
        ],
    ];

    /**
     * Translate mysql data types into postgresql data types.
     *
     * @param string $strMySqlDataType
     * @return string
     */
    public static function map(string $strMySqlDataType): string
    {
        $strDataType = '';
        $arrDataType = '';
        $arrDataTypeDetails = explode(' ', $strMySqlDataType);
        $increaseOriginalSize = in_array('unsigned', $arrDataTypeDetails) ||
            in_array('zerofill', $arrDataTypeDetails);
        $strMySqlDataType = $arrDataTypeDetails[0];
        $strMySqlDataType = strtolower($strMySqlDataType);
        $firstOccurrence = strpos($strMySqlDataType, '(');

        return self::getMySqlPgSqlTypesMap(
            $firstOccurrence,
            $increaseOriginalSize,
            $strDataType,
            $arrDataType,
            $strMySqlDataType
        );
    }

    /**
     * @param $firstOccurrence
     * @param $increaseOriginalSize
     * @param $strDataType
     * @param $arrDataType
     * @param $strMySqlDataType
     * @return string
     */
    public static function getMySqlPgSqlTypesMap(
        $firstOccurrence,
        $increaseOriginalSize,
        $strDataType,
        $arrDataType,
        $strMySqlDataType
    ): string {
        $lastOccurrence = false;
        if (false === $firstOccurrence) {
            // No parentheses detected.
            $strVal = $increaseOriginalSize
                ? self::$mySqlPgSqlTypesMap[$strMySqlDataType]['increased_size']
                : self::$mySqlPgSqlTypesMap[$strMySqlDataType]['type'];
        } else {
            // Parentheses detected.
            $lastOccurrence = strpos($strMySqlDataType, ')');
            $arrDataType = explode('(', $strMySqlDataType);
            $strDataType = strtolower($arrDataType[0]);
        }
        if ('enum' == $strDataType || 'set' == $strDataType) {
            $strVal = 'varchar(255)';
        } elseif ('decimal' == $strDataType || 'numeric' == $strDataType) {
            $strVal = self::getNumeric($strDataType, $arrDataType[1]);
        } elseif ('decimal(19,2)' == $strMySqlDataType) {
            $strVal = self::extractedDecimal($increaseOriginalSize, $strDataType);
        } elseif (self::$mySqlPgSqlTypesMap[$strDataType]['mySqlVarLenPgSqlFixedLen']) {
            // Should be converted without a length definition.
            $strVal = self::extractNoLength($increaseOriginalSize, $strDataType);
        } else {
            // Should be converted with a length definition.
            $strVal = self::extractWithLength($increaseOriginalSize, $strDataType, $arrDataType[1]);
        }


        return ' ' . strtoupper((new MapDataTypes())->verifyVal($strVal) . ' ');
    }
    public function verifyVal($strVal)
    {
        // Prevent incompatible length (CHARACTER(0) or CHARACTER VARYING(0)).
        switch ($strVal) {
            case 'character(0)':
                $strVal = 'character(1)';
                break;

            case 'character varying(0)':
                $strVal = 'character varying(1)';
                break;
        }
        return $strVal;
    }

    /**
     * @param $strDataType
     * @param $arrDataType
     *
     * @return string
     */
    public static function getNumeric($strDataType, $arrDataType): string
    {
        return self::$mySqlPgSqlTypesMap[$strDataType]['type'] . '(' . $arrDataType;
    }

    /**
     * @param $increaseOriginalSize
     * @param $strDataType
     *
     * @return mixed
     */
    public static function extractedDecimal($increaseOriginalSize, $strDataType)
    {
        return $increaseOriginalSize
            ? self::$mySqlPgSqlTypesMap[$strDataType]['increased_size']
            : self::$mySqlPgSqlTypesMap[$strDataType]['type'];
    }

    /**
     * @param $increaseOriginalSize
     * @param $strDataType
     *
     * @return mixed
     */
    public static function extractNoLength($increaseOriginalSize, $strDataType)
    {
        return $increaseOriginalSize
            ? self::$mySqlPgSqlTypesMap[$strDataType]['increased_size']
            : self::$mySqlPgSqlTypesMap[$strDataType]['type'];
    }

    /**
     * @param $increaseOriginalSize
     * @param $strDataType
     * @param $arrDataType
     *
     * @return string
     */
    public static function extractWithLength($increaseOriginalSize, $strDataType, $arrDataType): string
    {
        return $increaseOriginalSize
            ? self::$mySqlPgSqlTypesMap[$strDataType]['increased_size'] . '(' . $arrDataType
            : self::$mySqlPgSqlTypesMap[$strDataType]['type'] . '(' . $arrDataType;
    }
}
