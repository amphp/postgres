<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Statement {
    /**
     * @param mixed[] $params
     *
     * @return \Amp\Promise<\Amp\Sql\CommandResult>
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

    /**
     * @return int Timestamp of when the statement was last used.
     */
    public function lastUsedAt(): int;
}
