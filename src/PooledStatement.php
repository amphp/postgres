<?php

namespace Amp\Postgres;

use Amp\Promise;
use function Amp\call;

final class PooledStatement implements Statement {
    /** @var \Amp\Postgres\Pool */
    private $pool;

    /** @var \Amp\Postgres\Statement */
    private $statement;

    /**
     * @param \Amp\Postgres\Pool $pool Pool used to re-create the statement if the original closes.
     * @param \Amp\Postgres\Statement $statement
     */
    public function __construct(Pool $pool, Statement $statement) {
        $this->statement = $statement;
        $this->pool = $pool;
    }

    /**
     * {@inheritdoc}
     *
     * Unlike regular statements, as long as the pool is open this statement will not die.
     */
    public function execute(array $params = []): Promise {
        if ($this->statement->isAlive()) {
            return $this->statement->execute($params);
        }

        return call(function () use ($params) {
            $this->statement = yield $this->pool->prepare($this->statement->getQuery());
            return yield $this->statement->execute($params);
        });
    }

    /** {@inheritdoc} */
    public function isAlive(): bool {
        return $this->pool->isAlive();
    }

    /** {@inheritdoc} */
    public function getQuery(): string {
        return $this->statement->getQuery();
    }
}
