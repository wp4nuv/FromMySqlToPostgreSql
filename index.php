<?php

use App\FromMySqlToPostgreSql;

require './vendor/autoload.php';
require_once('migration/FromMySqlToPostgreSql/FromMySqlToPostgreSql.php');


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


    $arrRetVal = [];
    $config = '';

if (is_file($argv[1])) {
    $strExtension = pathinfo($argv[1], PATHINFO_EXTENSION);
    switch ($strExtension) {
        case 'json':
            $strContents = file_get_contents($argv[1]);
            $config = json_decode($strContents, true);
            $strError = '';

            switch (json_last_error()) {
                case JSON_ERROR_NONE:
                    // No code should be put here.
                    break;

                case JSON_ERROR_DEPTH:
                    $strError = 'Maximum stack depth exceeded';
                    break;

                case JSON_ERROR_STATE_MISMATCH:
                    $strError = 'Underflow or the modes mismatch';
                    break;

                case JSON_ERROR_CTRL_CHAR:
                    $strError = 'Unexpected control character found';
                    break;

                case JSON_ERROR_SYNTAX:
                    $strError = 'Syntax error, malformed JSON';
                    break;

                case JSON_ERROR_UTF8:
                    $strError = 'Malformed UTF-8 characters, possibly incorrectly encoded';
                    break;

                default:
                    $strError = 'Unknown error';
                    break;
            }
            $arrRetVal = is_null($config) || !empty($strError) ? [] : $config;
            break;
        case 'xml':
            $config = simplexml_load_file($argv[1]);
            $arrRetVal = empty($config) ? [] : get_object_vars($config);
            break;
    }
}

if (empty($argv[1])) {
    echo PHP_EOL, '-- Cannot perform a migration due to missing "config[.xml | .json]" file.', PHP_EOL;
} else {
    $arrConfig['config'] = $config;
    $arrConfig['temp_dir_path'] = __DIR__ . '/temp';


    $migration = new FromMySqlToPostgreSql($arrConfig);
    $migration->migrate();
}

    return $arrRetVal;
