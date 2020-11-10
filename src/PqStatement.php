<?php

namespace Amp\Postgres;

use Amp\Sql\Result;
use Amp\Sql\Statement;

final class PqStatement implements Statement
{
    private PqHandle $handle;

    private string $name;

    private string $sql;

    private array $params;

    private int $lastUsedAt;

    /**
     * @param PqHandle $handle
     * @param string $name Statement name.
     * @param string $sql Original prepared SQL query.
     * @param string[] $params Parameter indices to parameter names.
     */
    public function __construct(PqHandle $handle, string $name, string $sql, array $params)
    {
        $this->handle = $handle;
        $this->name = $name;
        $this->params = $params;
        $this->sql = $sql;
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
        $this->lastUsedAt = \time();
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }
}
