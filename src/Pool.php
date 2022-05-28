<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\Common\StatementPool;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Result;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionIsolation;
use Amp\Sql\TransactionIsolationLevel;
use function Amp\async;

final class Pool extends ConnectionPool implements Link
{
    /** @var Future<Connection>|null Connection used for notification listening. */
    private Future|null $listeningConnection = null;

    /** @var int Number of listeners on listening connection. */
    private int $listenerCount = 0;

    /**
     * @param positive-int $maxConnections
     * @param positive-int $idleTimeout
     * @param bool $resetConnections True to automatically execute DISCARD ALL on a connection before use.
     */
    public function __construct(
        PostgresConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        private readonly bool $resetConnections = true,
        ?PostgresConnector $connector = null,
    ) {
        parent::__construct($config, $connector ?? connector(), $maxConnections, $idleTimeout);
    }

    /**
     * @param \Closure():void $release
     */
    protected function createStatement(SqlStatement $statement, \Closure $release): SqlStatement
    {
        return new PooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, string $sql, \Closure $prepare): StatementPool
    {
        return new StatementPool($pool, $sql, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, \Closure $release): Transaction
    {
        \assert($transaction instanceof Transaction);
        return new PooledTransaction($transaction, $release);
    }

    /**
     * Changes return type to this library's Transaction type.
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    public function beginTransaction(
        TransactionIsolation $isolation = TransactionIsolationLevel::Committed
    ): Transaction {
        return parent::beginTransaction($isolation);
    }

    protected function pop(): Connection
    {
        $connection = parent::pop();
        \assert($connection instanceof Connection);

        if ($this->resetConnections) {
            $connection->query("DISCARD ALL");
        }

        return $connection;
    }

    public function notify(string $channel, string $payload = ""): Result
    {
        $connection = $this->pop();

        try {
            $result = $connection->notify($channel, $payload);
        } finally {
            $this->push($connection);
        }

        return $result;
    }

    public function listen(string $channel): Listener
    {
        $this->listeningConnection ??= async($this->pop(...));

        $connection = $this->listeningConnection->await();

        ++$this->listenerCount;

        try {
            $listener = $connection->listen($channel);
        } catch (\Throwable $exception) {
            if (--$this->listenerCount === 0) {
                $this->push($connection);
            }
            throw $exception;
        }

        return new PooledListener($listener, function () use ($connection): void {
            if (--$this->listenerCount === 0) {
                $this->push($connection);
            }
        });
    }
}
