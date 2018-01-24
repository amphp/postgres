<?php

namespace Amp\Postgres;

use Amp\Promise;
use pq;

class PqStatement implements Statement {
    /** @var \pq\Statement */
    private $statement;

    /** @var callable */
    private $execute;

    /** @var callable */
    private $deallocate;

    /** @var \Amp\Postgres\Internal\ReferenceQueue */
    private $queue;

    /** @var array */
    private $names;

    /**
     * @internal
     *
     * @param \pq\Statement $statement
     * @param string[] $names Parameter indices to parameter names.
     * @param callable $execute
     * @param callable $deallocate
     */
    public function __construct(pq\Statement $statement, array $names, callable $execute, callable $deallocate) {
        $this->statement = $statement;
        $this->names = $names;
        $this->execute = $execute;
        $this->deallocate = $deallocate;
        $this->queue = new Internal\ReferenceQueue;
    }

    public function __destruct() {
        ($this->deallocate)($this->statement->name);
        $this->queue->unreference();
    }

    /**
     * @return string
     */
    public function getQuery(): string {
        return $this->statement->query;
    }

    /**
     * @param mixed ...$params
     *
     * @return \Amp\Promise<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException If executing the statement fails.
     */
    public function execute(array $params = []): Promise {
        return ($this->execute)([$this->statement, "execAsync"], Internal\replaceNamedParams($params, $this->names));
    }

    /**
     * @param callable $onDestruct
     */
    public function onDestruct(callable $onDestruct) {
        $this->queue->onDestruct($onDestruct);
    }
}
