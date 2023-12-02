<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\Postgres\Internal\PostgresHandleConnection;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;

/**
 * @implements SqlConnector<PostgresConfig, PostgresHandleConnection>
 */
final class DefaultPostgresConnector implements SqlConnector
{
    /**
     * @throws SqlException If connecting fails.
     *
     * @throws \Error If neither ext-pgsql nor pecl-pq is loaded.
     */
    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): PostgresHandleConnection
    {
        if (!$config instanceof PostgresConfig) {
            throw new \TypeError(\sprintf("Must provide an instance of %s to Postgres connectors", PostgresConfig::class));
        }

        if (\extension_loaded("pq")) {
            return PqConnection::connect($config, $cancellation);
        }

        if (\extension_loaded("pgsql")) {
            return PgSqlConnection::connect($config, $cancellation);
        }

        throw new \Error("amphp/postgres requires either pecl-pq or ext-pgsql");
    }
}
