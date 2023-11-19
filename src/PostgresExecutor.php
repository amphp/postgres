<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\ConnectionException;
use Amp\Sql\Executor;
use Amp\Sql\SqlException;

/**
 * @extends Executor<PostgresResult, PostgresStatement>
 */
interface PostgresExecutor extends Executor, PostgresQuoter
{
    /**
     * @param non-empty-string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @throws SqlException If the operation fails due to unexpected condition.
     * @throws ConnectionException If the connection to the database is lost.
     */
    public function notify(string $channel, string $payload = ""): PostgresResult;
}
