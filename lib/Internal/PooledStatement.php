<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\Connection;
use Amp\Postgres\Statement;
use Amp\Promise;

class PooledStatement implements Statement {
    /** @var \Amp\Postgres\Connection */
    private $connection;

    /** @var \Amp\Postgres\Statement */
    private $statement;

    /** @var callable */
    private $push;

    /**
     * @param \Amp\Postgres\Connection $connection
     * @param \Amp\Postgres\Statement $statement
     * @param callable $push
     */
    public function __construct(Connection $connection, Statement $statement, callable $push) {
        $this->connection = $connection;
        $this->statement = $statement;
        $this->push = $push;
    }

    public function __destruct() {
        ($this->push)($this->connection);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(...$params): Promise {
        return $this->statement->execute(...$params);
    }
}
