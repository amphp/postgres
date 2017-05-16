<?php

namespace Amp\Postgres;

use Amp\Promise;

class PgSqlStatement implements Statement {
    /** @var string */
    private $sql;

    /** @var callable */
    private $execute;

    /**
     * @param string $sql
     * @param callable $execute
     */
    public function __construct(string $sql, callable $execute) {
        $this->sql = $sql;
        $this->execute = $execute;
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
    public function execute(...$params): Promise {
        return ($this->execute)($this->sql, $params);
    }
}
