<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\Connection;
use Amp\Postgres\Transaction;
use Amp\Promise;

class PooledConnection implements Connection {
    /** @var \Amp\Postgres\AbstractConnection */
    private $connection;

    /** @var callable $push */
    private $push;

    /**
     * @internal
     *
     * @param \Amp\Postgres\Connection $connection
     * @param callable $push
     */
    public function __construct(Connection $connection, callable $push) {
        $this->connection = $connection;
        $this->push = $push;
    }

    public function __destruct() {
        ($this->push)($this->connection);
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return $this->connection->transaction($isolation);
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise {
        return $this->connection->listen($channel);
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return $this->connection->query($sql);
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, ...$params): Promise {
        return $this->connection->execute($sql, ...$params);
    }

    /**
    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return $this->connection->prepare($sql);
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return $this->connection->notify($channel, $payload);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteString(string $data): string {
        return $this->connection->quoteString($data);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteName(string $name): string {
        return $this->connection->quoteName($name);
    }
}
