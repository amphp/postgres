<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\PooledStatement;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\Result;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;
use function Amp\async;
use function Amp\await;

final class Pool extends ConnectionPool implements Link
{
    /** @var Connection|Promise|null Connection used for notification listening. */
    private Connection|Promise|null $listeningConnection = null;

    /** @var int Number of listeners on listening connection. */
    private int $listenerCount = 0;

    private bool $resetConnections;

    /**
     * @param ConnectionConfig $config
     * @param int              $maxConnections
     * @param int              $idleTimeout
     * @param bool             $resetConnections True to automatically execute DISCARD ALL on a connection before use.
     * @param Connector|null   $connector
     */
    public function __construct(
        ConnectionConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        bool $resetConnections = true,
        ?Connector $connector = null
    ) {
        parent::__construct($config, $maxConnections, $idleTimeout, $connector);

        $this->resetConnections = $resetConnections;
    }

    /**
     * @return Connector The Connector instance defined by the connector() function.
     */
    protected function createDefaultConnector(): Connector
    {
        return connector();
    }

    protected function createStatement(SqlStatement $statement, callable $release): SqlStatement
    {
        return new PooledStatement($statement, $release);
    }

    protected function createStatementPool(SqlPool $pool, SqlStatement $statement, callable $prepare): SqlStatementPool
    {
        return new StatementPool($pool, $statement, $prepare);
    }

    protected function createTransaction(SqlTransaction $transaction, callable $release): SqlTransaction
    {
        \assert($transaction instanceof Transaction);
        return new PooledTransaction($transaction, $release);
    }

    protected function pop(): Connection
    {
        $connection = parent::pop();

        if ($this->resetConnections) {
            $connection->query("DISCARD ALL");
        }

        return $connection;
    }

    /**
     * @inheritDoc
     */
    public function notify(string $channel, string $payload = ""): Result
    {
        $connection = $this->pop();
        \assert($connection instanceof Connection);

        try {
            $result = $connection->notify($channel, $payload);
        } finally {
            $this->push($connection);
        }

        return $result;
    }

    /**
     * @inheritDoc
     */
    public function listen(string $channel): Listener
    {
        ++$this->listenerCount;

        if ($this->listeningConnection === null) {
            $this->listeningConnection = async(fn() => $this->pop());
        }

        if ($this->listeningConnection instanceof Promise) {
            $this->listeningConnection = await($this->listeningConnection);
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
