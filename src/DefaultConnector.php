<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Promise;

final class DefaultConnector implements Connector {
    /**
     * {@inheritdoc}
     *
     * @throws \Amp\Postgres\FailureException If connecting fails.
     * @throws \Error If neither ext-pgsql or pecl-pq is loaded.
     */
    public function connect(string $connectionString, CancellationToken $token = null): Promise {
        if (\extension_loaded("pq")) {
            return PqConnection::connect($connectionString, $token);
        }

        if (\extension_loaded("pgsql")) {
            return PgSqlConnection::connect($connectionString, $token);
        }

        throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
    }
}
