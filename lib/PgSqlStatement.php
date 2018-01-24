<?php

namespace Amp\Postgres;

use Amp\Promise;

class PgSqlStatement implements Statement {
    /** @var string */
    private $name;

    /** @var string */
    private $sql;

    /** @var callable */
    private $execute;

    /** @var callable */
    private $deallocate;

    /** @var \Amp\Postgres\Internal\ReferenceQueue */
    private $queue;

    /** @var string[] */
    private $names;

    /**
     * @internal
     *
     * @param string $name
     * @param string $sql
     * @param callable $execute
     * @param callable $deallocate
     */
    public function __construct(string $name, string $sql, array $names, callable $execute, callable $deallocate) {
        $this->name = $name;
        $this->sql = $sql;
        $this->names = $names;
        $this->execute = $execute;
        $this->deallocate = $deallocate;
        $this->queue = new Internal\ReferenceQueue;
    }

    public function __destruct() {
        ($this->deallocate)($this->name);
        $this->queue->unreference();
    }

    /**
     * @return string
     */
    public function getQuery(): string {
        return $this->sql;
    }

    /**
     * @param mixed ...$params
     *
     * @return \Amp\Promise<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException If executing the statement fails.
     */
    public function execute(array $params = []): Promise {
        return ($this->execute)($this->name, Internal\replaceNamedParams($params, $this->names));
    }

    /**
     * @param callable $onDestruct
     */
    public function onDestruct(callable $onDestruct) {
        $this->queue->onDestruct($onDestruct);
    }
}
