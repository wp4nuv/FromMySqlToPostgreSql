<?php

namespace App;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use PDOException;

class Logs
{
    private string $writeCommonLogTo;
    private string $writeSummaryTo;
    public string $writeErrorLogTo;

    public string $logsDirectoryPath;
    /**
     * @var false|resource
     */
    private $errorLog;
    /**
     * @var false|resource
     */
    private $commonLog;
    /**
     * @var \Monolog\Logger
     */
    private Logger $viewsLog;
    private string $writeViewLogTo;

    public function __construct()
    {
        $this->logsDirectoryPath = DIR(__FILE__, 2) . '/logs';
        $this->writeCommonLogTo = $this->logsDirectoryPath . '/all.log';
        $this->writeSummaryTo = $this->logsDirectoryPath . '/report-only.log';
        $this->writeErrorLogTo = $this->logsDirectoryPath . '/errors-only.log';
        $this->writeViewLogTo = $this->logsDirectoryPath . '/views.log';
        if (!file_exists($this->logsDirectoryPath)) {
            try {
                mkdir($this->logsDirectoryPath);
            } catch (\Exception $exception) {
                echo PHP_EOL, '--Cannot create logs directory: ', $this->logsDirectoryPath, PHP_EOL;
            }
        }
        $this->errorLog = new Logger('error');
        $this->commonLog = new Logger('common');
        $this->viewsLog = new Logger('views');

        $this->errorLog->pushHandler(new StreamHandler($this->writeErrorLogTo, Logger::ERROR));
        $this->commonLog->pushHandler(new StreamHandler($this->writeCommonLogTo, Logger::INFO));
        $this->viewsLog->pushHandler(new StreamHandler($this->writeViewLogTo, Logger::WARNING));
    }

    /**
     * Outputs given log.
     * Writes given string to the "common logs" file.
     *
     * @param string $log The string value to log
     * @param string $type To which error are we writing? Defaults to 'error'
     * @return void
     */
    public function write(string $log, string $type = 'error')
    {
        switch ($type) {
            case 'common':
                $this->commonLog->notice($log);
                break;
            case 'view':
                $this->viewsLog->warning($log);
                break;
            default:
                $this->errorLog->error($log);
                break;
        }
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

        $this->write($strError);
        unset($strError);
    }
}
