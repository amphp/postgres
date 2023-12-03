<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Future;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\Result;
use Amp\Sql\SqlConnector;
use Amp\Sql\Statement;
use Amp\Sql\Transaction;
use function Amp\async;

/**
 * @extends ConnectionPool<PostgresConfig, PostgresResult, PostgresStatement, PostgresTransaction, PostgresConnection>
 */
final class PostgresConnectionPool extends ConnectionPool implements PostgresConnection
{
    /** @var Future<PostgresConnection>|null Connection used for notification listening. */
    private Future|null $listeningConnection = null;

    /** @var int Number of listeners on listening connection. */
    private int $listenerCount = 0;

    /**
     * @param positive-int $maxConnections
     * @param positive-int $idleTimeout
     * @param bool $resetConnections True to automatically execute DISCARD ALL on a connection before use.
     * @param SqlConnector<PostgresConfig, PostgresConnection>|null $connector
     */
    public function __construct(
        PostgresConfig $config,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        int $idleTimeout = self::DEFAULT_IDLE_TIMEOUT,
        private readonly bool $resetConnections = true,
        ?SqlConnector $connector = null,
    ) {
        parent::__construct($config, $connector ?? postgresConnector(), $maxConnections, $idleTimeout);
    }

    /**
     * @param \Closure():void $release
     */
    protected function createStatement(Statement $statement, \Closure $release): PostgresStatement
    {
        \assert($statement instanceof PostgresStatement);
        return new Internal\PostgresPooledStatement($statement, $release);
    }

    protected function createResult(Result $result, \Closure $release): PostgresResult
    {
        \assert($result instanceof PostgresResult);
        return new Internal\PostgresPooledResult($result, $release);
    }

    protected function createStatementPool(string $sql, \Closure $prepare): PostgresStatement
    {
        return new Internal\PostgresStatementPool($this, $sql, $prepare);
    }

    protected function createTransaction(Transaction $transaction, \Closure $release): PostgresTransaction
    {
        \assert($transaction instanceof PostgresTransaction);
        return new Internal\PostgresPooledTransaction($transaction, $release);
    }

    protected function pop(): PostgresConnection
    {
        $connection = parent::pop();

        if ($this->resetConnections) {
            $connection->query("DISCARD ALL");
        }

        return $connection;
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function query(string $sql): PostgresResult
    {
        return parent::query($sql);
    }

    /**
     * Changes return type to this library's Statement type.
     */
    public function prepare(string $sql): PostgresStatement
    {
        return parent::prepare($sql);
    }

    /**
     * Changes return type to this library's Result type.
     */
    public function execute(string $sql, array $params = []): PostgresResult
    {
        return parent::execute($sql, $params);
    }

    /**
     * Changes return type to this library's Transaction type.
     */
    public function beginTransaction(): PostgresTransaction
    {
        return parent::beginTransaction();
    }

    /**
     * Changes return type to this library's configuration type.
     */
    public function getConfig(): PostgresConfig
    {
        return parent::getConfig();
    }

    public function notify(string $channel, string $payload = ""): PostgresResult
    {
        $connection = $this->pop();

        try {
            $result = $connection->notify($channel, $payload);
        } finally {
            $this->push($connection);
        }

        return $result;
    }

    public function listen(string $channel): PostgresListener
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

        return new Internal\PostgresPooledListener($listener, function () use ($connection): void {
            if (--$this->listenerCount === 0) {
                $this->push($connection);
            }
        });
    }

    public function quoteString(string $data): string
    {
        $connection = $this->pop();

        try {
            return $connection->quoteString($data);
        } finally {
            $this->push($connection);
        }
    }

    public function quoteName(string $name): string
    {
        $connection = $this->pop();

        try {
            return $connection->quoteName($name);
        } finally {
            $this->push($connection);
        }
    }

    public function escapeByteA(string $data): string
    {
        $connection = $this->pop();

        try {
            return $connection->escapeByteA($data);
        } finally {
            $this->push($connection);
        }
    }
}
