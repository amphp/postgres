<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Executor as SqlExecutor;

interface Executor extends SqlExecutor
{
    const STATEMENT_NAME_PREFIX = "amp_";

    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     */
    public function notify(string $channel, string $payload = ""): Promise;
}
