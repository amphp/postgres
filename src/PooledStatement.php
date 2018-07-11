<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Statement;

final class PooledStatement implements Statement
{
    /** @var Statement */
    private $statement;

    /** @var callable|null */
    private $release;

    public function __construct(Statement $statement, callable $release)
    {
        $this->statement = $statement;
        $this->release = $release;

        if (!$this->statement->isAlive()) {
            ($this->release)();
            $this->release = null;
        }
    }

    public function __destruct()
    {
        ($this->release)();
    }

    public function execute(array $params = []): Promise
    {
        return $this->statement->execute($params);
    }

    public function isAlive(): bool
    {
        return $this->statement->isAlive();
    }

    public function getQuery(): string
    {
        return $this->statement->getQuery();
    }

    public function lastUsedAt(): int
    {
        return $this->statement->lastUsedAt();
    }
}
