<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\Connection;
use Amp\Sql\ConnectionException;
use Amp\Sql\QueryError;
use Amp\Sql\SqlException;

/**
 * @extends Connection<PostgresConfig, PostgresResult, PostgresStatement, PostgresTransaction>
 */
interface PostgresConnection extends Connection, PostgresLink
{
    /**
     * @return PostgresConfig Config object specific to this library.
     */
    public function getConfig(): PostgresConfig;

    /**
     * @param non-empty-string $channel Channel name.
     *
     * @throws SqlException If the operation fails due to unexpected condition.
     * @throws ConnectionException If the connection to the database is lost.
     * @throws QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function listen(string $channel): PostgresListener;
}
