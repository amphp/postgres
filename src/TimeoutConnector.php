<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\FailureException;
use Amp\TimeoutCancellationToken;

final class TimeoutConnector implements Connector
{
    const DEFAULT_TIMEOUT = 5000;

    /** @var int */
    private $timeout;

    /**
     * @param int $timeout Milliseconds until connections attempts are cancelled.
     */
    public function __construct(int $timeout = self::DEFAULT_TIMEOUT)
    {
        $this->timeout = $timeout;
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException If connecting fails.
     *
     * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
     */
    public function connect(SqlConnectionConfig $connectionConfig): Promise
    {
        if (!$connectionConfig instanceof ConnectionConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to Postgres connectors", ConnectionConfig::class));
        }

        $token = new TimeoutCancellationToken($this->timeout);

        if (\extension_loaded("pq")) {
            return PqConnection::connect($connectionConfig, $token);
        }

        if (\extension_loaded("pgsql")) {
            return PgSqlConnection::connect($connectionConfig, $token);
        }

        throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
    }
}
