<?php

namespace Amp\Postgres;

use Amp\Sql\Result;
use Amp\Sql\Statement;

final class PqStatement implements Statement
{
    private int $lastUsedAt;

    /**
     * @param string $name Statement name.
     * @param string $sql Original prepared SQL query.
     * @param array<int, int|string> $params Parameter indices to parameter names.
     */
    public function __construct(
        private readonly PqHandle $handle,
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
        $this->lastUsedAt = \time();
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }
}
