<?php

namespace App\Utility;

class Utility
{

    /**
     * Outputs given log.
     * Writes given string to the "common logs" file.
     *
     * @param string $strLog
     * @param bool $boolIsError
     * @return void
     */
    public function log(string $strLog, bool $boolIsError = false): void
    {
        if (!$boolIsError) {
            echo $strLog;
        }

        if (!empty($this->strWriteCommonLogTo)) {
            if (is_resource($this->resourceCommonLog)) {
                fwrite($this->resourceCommonLog, $strLog);
            } else {
                $this->resourceCommonLog = fopen($this->strWriteCommonLogTo, 'a');

                if (is_resource($this->resourceCommonLog)) {
                    fwrite($this->resourceCommonLog, $strLog);
                }
            }
        }
    }
}