<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Statement;

final class PqStatement implements Statement
{
    /** @var PqHandle */
    private $handle;

    /** @var string */
    private $name;

    /** @var string */
    private $sql;

    /** @var array */
    private $params;

    /** @var int */
    private $lastUsedAt;

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
        $this->lastUsedAt = \time();
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }
}
