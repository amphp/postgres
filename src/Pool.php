<?php

namespace Amp\Postgres;

use Amp\Future;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Result;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;
use Amp\Sql\TransactionIsolation;
use function Amp\async;

final class Pool extends ConnectionPool implements Link
{
    /** @var Connection|Future|null Connection used for notification listening. */
    private Connection|Future|null $listeningConnection = null;

    /** @var int Number of listeners on listening connection. */
    private int $listenerCount = 0;

    private readonly bool $resetConnections;

    /**
     * @param bool             $resetConnections True to automatically execute DISCARD ALL on a connection before use.
     */
    public function __construct(
        PostgresConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        bool $resetConnections = true,
        ?PostgresConnector $connector = null
    ) {
        parent::__construct($config, $maxConnections, $idleTimeout, $connector);

        $this->resetConnections = $resetConnections;
    }

    /**
     * @return PostgresConnector The Connector instance defined by the connector() function.
     */
    protected function createDefaultConnector(): PostgresConnector
    {
        return connector();
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
     */
    public function beginTransaction(TransactionIsolation $isolation = TransactionIsolation::Committed): Transaction
    {
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
        ++$this->listenerCount;

        if ($this->listeningConnection === null) {
            $this->listeningConnection = async(fn () => $this->pop());
        }

        if ($this->listeningConnection instanceof Future) {
            $this->listeningConnection = $this->listeningConnection->await();
        }

        try {
            $listener = $this->listeningConnection->listen($channel);
        } catch (\Throwable $exception) {
            if (--$this->listenerCount === 0) {
                $connection = $this->listeningConnection;
                $this->listeningConnection = null;
                $this->push($connection);
            }
            throw $exception;
        }

        return new PooledListener($listener, function (): void {
            if (--$this->listenerCount === 0) {
                $connection = $this->listeningConnection;
                $this->listeningConnection = null;
                $this->push($connection);
            }
        });
    }
}
