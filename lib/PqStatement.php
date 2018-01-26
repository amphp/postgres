<?php

namespace Amp\Postgres;

use Amp\Promise;

class PqStatement implements Statement, Operation {
    /** @var \Amp\Postgres\PqHandle */
    private $handle;

    /** @var string */
    private $name;

    /** @var string */
    private $sql;

    /** @var \Amp\Postgres\Internal\ReferenceQueue */
    private $queue;

    /** @var array */
    private $params;

    /**
     * @param \Amp\Postgres\PqHandle $handle
     * @param string $name Statement name.
     * @param string $sql Original prepared SQL query.
     * @param string[] $params Parameter indices to parameter names.
     */
    public function __construct(PqHandle $handle, string $name, string $sql, array $params) {
        $this->handle = $handle;
        $this->name = $name;
        $this->params = $params;
        $this->sql = $sql;
        $this->queue = new Internal\ReferenceQueue;
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
    public function execute(array $params = []): Promise {
        return $this->handle->statementExecute($this->name, Internal\replaceNamedParams($params, $this->params));
    }

    /** {@inheritdoc} */
    public function onDestruct(callable $onDestruct) {
        $this->queue->onDestruct($onDestruct);
    }
}
