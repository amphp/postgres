<?php

namespace Amp\Postgres;

use Amp\Sql\ConnectionException;
use Amp\Sql\Executor as SqlExecutor;
use Amp\Sql\FailureException;
use Amp\Sql\Result;

interface Executor extends SqlExecutor
{
    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @throws FailureException If the operation fails due to unexpected condition.
     * @throws ConnectionException If the connection to the database is lost.
     */
    public function notify(string $channel, string $payload = ""): Result;
}
