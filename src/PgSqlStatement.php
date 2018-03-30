<?php

namespace Amp\Postgres;

use Amp\Promise;

final class PgSqlStatement implements Statement, Operation {
    /** @var \Amp\Postgres\PgSqlHandle */
    private $handle;

    /** @var string */
    private $name;

    /** @var string */
    private $sql;

    /** @var \Amp\Postgres\Internal\ReferenceQueue */
    private $queue;

    /** @var string[] */
    private $params;

    /** @var int */
    private $lastUsedAt;

    /**
     * @param \Amp\Postgres\PgSqlHandle $handle
     * @param string $name
     * @param string $sql
     * @param string[] $params
     */
    public function __construct(PgSqlHandle $handle, string $name, string $sql, array $params) {
        $this->handle = $handle;
        $this->name = $name;
        $this->sql = $sql;
        $this->params = $params;
        $this->queue = new Internal\ReferenceQueue;
        $this->lastUsedAt = \time();
    }

    public function __destruct() {
        $this->handle->statementDeallocate($this->name);
        $this->queue->unreference();
    }

    /** {@inheritdoc} */
    public function isAlive(): bool {
        return $this->handle->isAlive();
    }

    /** {@inheritdoc} */
    public function getQuery(): string {
        return $this->sql;
    }

    /** {@inheritdoc} */
    public function lastUsedAt(): int {
        return $this->lastUsedAt;
    }

    /** {@inheritdoc} */
    public function execute(array $params = []): Promise {
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }

    /** {@inheritdoc} */
    public function onDestruct(callable $onDestruct) {
        $this->queue->onDestruct($onDestruct);
    }
}
