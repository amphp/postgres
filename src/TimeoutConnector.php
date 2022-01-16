<?php

namespace Amp\Postgres;

use Amp\Sql\ConnectionConfig as SqlConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\FailureException;
use Amp\TimeoutCancellation;

final class TimeoutConnector implements Connector
{
    const DEFAULT_TIMEOUT = 5;

    private int $timeout;

    /**
     * @param int $timeout Milliseconds until connections attempts are cancelled.
     */
    public function __construct(int $timeout = self::DEFAULT_TIMEOUT)
    {
        $this->timeout = $timeout;
    }

    /**
     * @inheritDoc
     *
     * @throws FailureException If connecting fails.
     *
     * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
     */
    public function connect(SqlConnectionConfig $config): Connection
    {
        if (!$config instanceof ConnectionConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to Postgres connectors", ConnectionConfig::class));
        }

        $cancellation = new TimeoutCancellation($this->timeout, "Connecting to the Postgres database timed out");

        if (\extension_loaded("pq")) {
            return PqConnection::connect($config, $cancellation);
        }

        if (\extension_loaded("pgsql")) {
            return PgSqlConnection::connect($config, $cancellation);
        }

        throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
    }
}
