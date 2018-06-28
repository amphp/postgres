<?php

namespace Amp\Postgres;

use function Amp\call;
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
        return call(function () use ($connectionConfig) {
            $token = new TimeoutCancellationToken($this->timeout);

            if (\extension_loaded("pq")) {
                $connection = new PqConnection($connectionConfig, $token);

                yield $connection->connect();

                return $connection;
            }

            if (\extension_loaded("pgsql")) {
                $connection = new PgSqlConnection($connectionConfig, $token);

                yield $connection->connect();

                return $connection;
            }

            throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
        });
    }
}
