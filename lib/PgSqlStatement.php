<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Awaitable;

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
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException If executing the statement fails.
     */
    public function execute(...$params): Awaitable {
        return ($this->execute)($this->sql, $params);
    }
}