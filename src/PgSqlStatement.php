<?php

namespace Amp\Postgres;

use Amp\Sql\Result;
use Amp\Sql\Statement;

final class PgSqlStatement implements Statement
{
    private int $lastUsedAt;

    /**
     * @param array<int, int|string> $params
     */
    public function __construct(
        private readonly PgSqlHandle $handle,
        private readonly string $name,
        private readonly string $sql,
        private readonly array $params,
    ) {
        $this->lastUsedAt = \time();
    }

    public function __destruct()
    {
        $this->handle->statementDeallocate($this->name);
    }

    public function isAlive(): bool
    {
        return $this->handle->isAlive();
    }

    public function getQuery(): string
    {
        return $this->sql;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    public function execute(array $params = []): Result
    {
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }
}
