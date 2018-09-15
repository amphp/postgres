<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Statement;

final class PgSqlStatement implements Statement
{
    /** @var PgSqlHandle */
    private $handle;

    /** @var string */
    private $name;

    /** @var string */
    private $sql;

    /** @var string[] */
    private $params;

    /** @var int */
    private $lastUsedAt;

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

    /** {@inheritdoc} */
    public function isAlive(): bool
    {
        return $this->handle->isAlive();
    }

    /** {@inheritdoc} */
    public function getQuery(): string
    {
        return $this->sql;
    }

    /** {@inheritdoc} */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /** {@inheritdoc} */
    public function execute(array $params = []): Promise
    {
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }
}
