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

    /**
     * @internal
     *
     * @param \pq\Statement $statement
     * @param callable $execute
     * @param callable $deallocate
     */
    public function __construct(pq\Statement $statement, callable $execute, callable $deallocate) {
        $this->statement = $statement;
        $this->execute = $execute;
        $this->deallocate = $deallocate;
    }

    public function __destruct() {
        ($this->deallocate)($this->statement->name);
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
        return ($this->execute)([$this->statement, "execAsync"], $params);
    }
}
