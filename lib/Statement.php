<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Statement {
    /**
     * @param mixed[] $params
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult|\Amp\Postgres\TupleResult>
     */
    public function execute(array $params = []): Promise;

    /**
     * @return bool True if the statement can still be executed, false if the connection has died.
     */
    public function isAlive(): bool;

    /**
     * @return string The SQL string used to prepare the statement.
     */
    public function getQuery(): string;
}
