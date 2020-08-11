<?php

namespace Amp\Postgres;

use Amp\Coroutine;
use Amp\Promise;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\ConnectionConfig;
use Amp\Sql\Connector;
use Amp\Sql\Pool as SqlPool;
use Amp\Sql\ResultSet as SqlResultSet;
use Amp\Sql\Statement as SqlStatement;
use Amp\Sql\Transaction as SqlTransaction;
use function Amp\call;

final class Pool extends ConnectionPool implements Link
{
    /** @var Connection|Promise|null Connection used for notification listening. */
    private $listeningConnection;

    /** @var int Number of listeners on listening connection. */
    private $listenerCount = 0;

    /** @var bool */
    private $resetConnections;

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

    protected function createResultSet(SqlResultSet $resultSet, callable $release): SqlResultSet
    {
        \assert($resultSet instanceof ResultSet);
        return new PooledResultSet($resultSet, $release);
    }

    protected function pop(): \Generator
    {
        $connection = yield from parent::pop();
        \assert($connection instanceof Connection);

        if ($this->resetConnections) {
            yield $connection->query("DISCARD ALL");
        }

        return $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        return call(function () use ($channel, $payload) {
            $connection = yield from $this->pop();
            \assert($connection instanceof Connection);

            try {
                $result = yield $connection->notify($channel, $payload);
            } finally {
                $this->push($connection);
            }

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise
    {
        return call(function () use ($channel) {
            ++$this->listenerCount;

            if ($this->listeningConnection === null) {
                $this->listeningConnection = new Coroutine($this->pop());
            }

            if ($this->listeningConnection instanceof Promise) {
                $this->listeningConnection = yield $this->listeningConnection;
            }

            try {
                $listener = yield $this->listeningConnection->listen($channel);
                \assert($listener instanceof Listener);
            } catch (\Throwable $exception) {
                if (--$this->listenerCount === 0) {
                    $connection = $this->listeningConnection;
                    $this->listeningConnection = null;
                    $this->push($connection);
                }
                throw $exception;
            }

            return new PooledListener($listener, function () {
                if (--$this->listenerCount === 0) {
                    $connection = $this->listeningConnection;
                    $this->listeningConnection = null;
                    $this->push($connection);
                }
            });
        });
    }
}
