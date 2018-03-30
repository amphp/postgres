<?php

namespace Amp\Postgres;

use Amp\Loop;
use Amp\Promise;
use function Amp\call;

final class PooledStatement implements Statement {
    /** @var \Amp\Postgres\Pool */
    private $pool;

    /** @var \SplQueue */
    private $statements;

    /** @var string */
    private $sql;

    /** @var int */
    private $lastUsedAt;

    /** @var string */
    private $timeoutWatcher;

    /** @var callable */
    private $prepare;

    /**
     * @param \Amp\Postgres\Pool $pool Pool used to re-create the statement if the original closes.
     * @param \Amp\Postgres\Statement $statement
     * @param callable $prepare
     */
    public function __construct(Pool $pool, Statement $statement, callable $prepare) {
        $this->lastUsedAt = \time();
        $this->statements = $statements = new \SplQueue;
        $this->pool = $pool;
        $this->prepare = $prepare;
        $this->sql = $statement->getQuery();

        $this->statements->push($statement);

        $this->timeoutWatcher = Loop::repeat(1000, static function () use ($pool, $statements) {
            $now = \time();
            $idleTimeout = $pool->getIdleTimeout();

            while (!$statements->isEmpty()) {
                /** @var \Amp\Postgres\Statement $statement */
                $statement = $statements->bottom();

                if ($statement->lastUsedAt() + $idleTimeout > $now) {
                    return;
                }

                $statements->shift();
            }
        });

        Loop::unreference($this->timeoutWatcher);
    }

    public function __destruct() {
        Loop::cancel($this->timeoutWatcher);
    }

    /**
     * {@inheritdoc}
     *
     * Unlike regular statements, as long as the pool is open this statement will not die.
     */
    public function execute(array $params = []): Promise {
        $this->lastUsedAt = \time();

        return call(function () use ($params) {
            if (!$this->statements->isEmpty()) {
                do {
                    /** @var \Amp\Postgres\Statement $statement */
                    $statement = $this->statements->shift();
                } while (!$statement->isAlive() && !$this->statements->isEmpty());
            } else {
                $statement = yield ($this->prepare)($this->sql);
            }

            try {
                $result = yield $statement->execute($params);
            } catch (\Throwable $exception) {
                $this->statements->push($statement);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($statement) {
                    $this->statements->push($statement);
                });
            } else {
                $this->statements->push($statement);
            }

            return $result;
        });
    }

    /** {@inheritdoc} */
    public function isAlive(): bool {
        return $this->pool->isAlive();
    }

    /** {@inheritdoc} */
    public function getQuery(): string {
        return $this->sql;
    }

    /** {@inheritdoc} */
    public function lastUsedAt(): int {
        return $this->lastUsedAt;
    }
}
