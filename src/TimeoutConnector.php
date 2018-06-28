<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\FailureException;
use Amp\TimeoutCancellationToken;

final class TimeoutConnector implements Connector {
    const DEFAULT_TIMEOUT = 5000;

    /** @var int */
    private $timeout;

    /**
     * @param int $timeout Milliseconds until connections attempts are cancelled.
     */
    public function __construct(int $timeout = self::DEFAULT_TIMEOUT) {
        $this->timeout = $timeout;
    }

    /**
     * {@inheritdoc}
     *
     * @throws FailureException If connecting fails.
     *
     * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
     */
    public function connect(ConnectionConfig $connectionConfig): Promise {
        $token = new TimeoutCancellationToken($this->timeout);
        $connectionString = $connectionConfig->connectionString();

        if (\extension_loaded("pq")) {
            return PqConnection::connect($connectionString, $token);
        }

        if (\extension_loaded("pgsql")) {
            return PgSqlConnection::connect($connectionString, $token);
        }

        throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
    }
}
