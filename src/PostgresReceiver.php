<?php

namespace Amp\Postgres;

use Amp\Sql\ConnectionException;
use Amp\Sql\QueryError;
use Amp\Sql\SqlException;

interface PostgresReceiver extends PostgresExecutor
{
    /**
     * @param non-empty-string $channel Channel name.
     *
     * @throws SqlException If the operation fails due to unexpected condition.
     * @throws ConnectionException If the connection to the database is lost.
     * @throws QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function listen(string $channel): PostgresListener;
}
