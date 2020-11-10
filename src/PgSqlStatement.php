<?php

namespace Amp\Postgres;

use Amp\Sql\Result;
use Amp\Sql\Statement;

final class PgSqlStatement implements Statement
{
    private PgSqlHandle $handle;

    private string $name;

    private string $sql;

    private array $params;

    private int $lastUsedAt;

    /**
     * @param PgSqlHandle $handle
     * @param string $name
     * @param string $sql
     * @param string[] $params
     */
    public function __construct(PgSqlHandle $handle, string $name, string $sql, array $params)
    {
        $this->handle = $handle;
        $this->name = $name;
        $this->sql = $sql;
        $this->params = $params;
        $this->lastUsedAt = \time();
    }

    public function __destruct()
    {
        $this->handle->statementDeallocate($this->name);
    }

    /** @inheritDoc */
    public function isAlive(): bool
    {
        return $this->handle->isAlive();
    }

    /** @inheritDoc */
    public function getQuery(): string
    {
        return $this->sql;
    }

    /** @inheritDoc */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /** @inheritDoc */
    public function execute(array $params = []): Result
    {
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }
}
