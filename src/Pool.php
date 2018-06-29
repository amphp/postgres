<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Loop;
use Amp\Promise;
use Amp\Sql\Connector;
use Amp\Sql\FailureException;
use Amp\Sql\Operation;
use Amp\Sql\Pool as SqlPool;
use function Amp\call;
use function Amp\coroutine;
use Amp\Sql\Statement;

final class Pool implements SqlPool
{
    use CallableMaker;

    const DEFAULT_MAX_CONNECTIONS = 100;
    const DEFAULT_IDLE_TIMEOUT = 60;

    /** @var Connector */
    private $connector;

    /** @var string */
    private $connectionString;

    /** @var int */
    private $maxConnections;

    /** @var \SplQueue */
    private $idle;

    /** @var \SplObjectStorage */
    private $connections;

    /** @var |null */
    private $promise;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var Connection||null Connection used for notification listening. */
    private $listeningConnection;

    /** @var int Number of listeners on listening connection. */
    private $listenerCount = 0;

    /** @var callable */
    private $prepare;

    /** @var int */
    private $pending = 0;

    /** @var bool */
    private $resetConnections = true;

    /** @var int */
    private $idleTimeout = self::DEFAULT_IDLE_TIMEOUT;

    /** @var string */
    private $timeoutWatcher;

    /** @var bool */
    private $closed = false;

    /** @var int */
    private $lastUsedAt;

    public function __construct(
        string $connectionString,
        int $maxConnections = self::DEFAULT_MAX_CONNECTIONS,
        Connector $connector = null
    ) {
        $this->connector = $connector ?? connector();

        $this->connectionString = $connectionString;

        $this->maxConnections = $maxConnections;
        if ($this->maxConnections < 1) {
            throw new \Error("Pool must contain at least one connection");
        }

        $this->connections = $connections = new \SplObjectStorage;
        $this->idle = $idle = new \SplQueue;
        $this->prepare = coroutine($this->callableFromInstanceMethod("doPrepare"));

        $idleTimeout = &$this->idleTimeout;

        $this->timeoutWatcher = Loop::repeat(1000, static function () use (&$idleTimeout, $connections, $idle) {
            $now = \time();
            while (!$idle->isEmpty()) {
                /** @var Connection $connection */
                $connection = $idle->bottom();

                if ($connection->lastUsedAt() + $idleTimeout > $now) {
                    return;
                }

                // Close connection and remove it from the pool.
                $idle->shift();
                $connections->detach($connection);
                $connection->close();
            }
        });

        Loop::unreference($this->timeoutWatcher);

        $this->lastUsedAt = \time();
    }

    public function __destruct() {
        Loop::cancel($this->timeoutWatcher);
    }

    public function resetConnections(bool $reset = true) {
        $this->resetConnections = $reset;
    }

    public function getIdleTimeout(): int {
        return $this->idleTimeout;
    }

    public function setIdleTimeout(int $timeout) {
        if ($timeout < 1) {
            throw new \Error("Timeout must be greater than or equal to 1");
        }

        $this->idleTimeout = $timeout;
    }

    /**
     * @return bool
     */
    public function isAlive(): bool {
        return !$this->closed;
    }

    public function lastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * Close all connections in the pool. No further queries may be made after a pool is closed.
     */
    public function close() {
        $this->closed = true;
        foreach ($this->connections as $connection) {
            $connection->close();
        }
        $this->idle = new \SplQueue;
        $this->connections = new \SplObjectStorage;
        $this->listeningConnection = null;
        $this->prepare = null;
    }

    /**
     * {@inheritdoc}
     */
    public function extractConnection(): Promise {
        return call(function () {
            $connection = yield from $this->pop();
            $this->connections->detach($connection);

            $this->lastUsedAt = \time();

            return $connection;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function getConnectionCount(): int {
        return $this->connections->count();
    }

    /**
     * {@inheritdoc}
     */
    public function getIdleConnectionCount(): int {
        return $this->idle->count();
    }

    /**
     * {@inheritdoc}
     */
    public function getMaxConnections(): int {
        return $this->maxConnections;
    }

    /**
     * @return \Generator
     *
     * @resolve Connection
     *
     * @throws FailureException If creating a new connection fails.
     * @throws \Error If the pool has been closed.
     */
    private function pop(): \Generator {
        if ($this->closed) {
            throw new \Error("The pool has been closed");
        }

        while ($this->promise !== null && $this->connections->count() + $this->pending >= $this->getMaxConnections()) {
            yield $this->promise; // Prevent simultaneous connection creation when connection count is at maximum - 1.
        }

        do {
            // While loop to ensure an idle connection is available after promises below are resolved.
            while ($this->idle->isEmpty()) {
                if ($this->connections->count() + $this->pending < $this->getMaxConnections()) {
                    // Max connection count has not been reached, so open another connection.
                    ++$this->pending;
                    try {
                        $connection = yield $this->connector->connect($this->connectionString);
                        if (!$connection instanceof Connection) {
                            throw new \Error(\sprintf(
                                "%s::createConnection() must resolve to an instance of %s",
                                static::class,
                                Connection::class
                            ));
                        }
                    } finally {
                        --$this->pending;
                    }

                    $this->connections->attach($connection);

                    $this->lastUsedAt = \time();

                    return $connection;
                }

                // All possible connections busy, so wait until one becomes available.
                try {
                    $this->deferred = new Deferred;
                    // May be resolved with defunct connection, but that connection will not be added to $this->idle.
                    yield $this->promise = $this->deferred->promise();
                } finally {
                    $this->deferred = null;
                    $this->promise = null;
                }
            }

            /** @var Connection $connection */
            $connection = $this->idle->shift();

            if ($connection->isAlive()) {
                try {
                    if ($this->resetConnections) {
                        yield $connection->query("RESET ALL");
                    }

                    $this->lastUsedAt = \time();

                    return $connection;
                } catch (FailureException $exception) {
                    // Fall-through to remove connection below.
                }
            }

            $this->connections->detach($connection);
        } while (!$this->closed);

        throw new FailureException("Pool closed before an active connection could be obtained");
    }

    /**
     * @param Connection $connection
     *
     * @throws \Error If the connection is not part of this pool.
     */
    private function push(Connection $connection) {
        \assert(isset($this->connections[$connection]), 'Connection is not part of this pool');

        if ($connection->isAlive()) {
            $this->idle->push($connection);
        } else {
            $this->connections->detach($connection);
        }

        if ($this->deferred instanceof Deferred) {
            $this->deferred->resolve($connection);
        }

        $this->lastUsedAt = \time();
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return call(function () use ($sql) {
            /** @var Connection $connection */
            $connection = yield from $this->pop();

            try {
                $result = yield $connection->query($sql);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($connection) {
                    $this->push($connection);
                });
            } else {
                $this->push($connection);
            }

            $this->lastUsedAt = \time();

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise {
        return call(function () use ($sql, $params) {
            /** @var Connection $connection */
            $connection = yield from $this->pop();

            try {
                $result = yield $connection->execute($sql, $params);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            if ($result instanceof Operation) {
                $result->onDestruct(function () use ($connection) {
                    $this->push($connection);
                });
            } else {
                $this->push($connection);
            }

            $this->lastUsedAt = \time();

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     *
     * Prepared statements returned by this method will stay alive as long as the pool remains open.
     */
    public function prepare(string $sql): Promise {
        return call(function () use ($sql) {
            $statement = yield from $this->doPrepare($sql);

            $this->lastUsedAt = \time();

            return new Internal\PooledStatement($this, $statement, $this->prepare);
        });
    }

    private function doPrepare(string $sql): \Generator {
        /** @var Connection $connection */
        $connection = yield from $this->pop();

        try {
            /** @var Statement $statement */
            $statement = yield $connection->prepare($sql);
        } catch (\Throwable $exception) {
            $this->push($connection);
            throw $exception;
        }

        \assert(
            $statement instanceof Operation,
            Statement::class . " instances returned from connections must implement " . Operation::class
        );

        $statement->onDestruct(function () use ($connection) {
            $this->push($connection);
        });

        $this->lastUsedAt = \time();

        return $statement;
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return call(function () use ($channel, $payload) {
            /** @var Connection $connection */
            $connection = yield from $this->pop();

            try {
                $result = yield $connection->notify($channel, $payload);
            } finally {
                $this->push($connection);
            }

            $this->lastUsedAt = \time();

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise {
        return call(function () use ($channel) {
            ++$this->listenerCount;

            if ($this->listeningConnection === null) {
                $this->listeningConnection = new Coroutine($this->pop());
            }

            if ($this->listeningConnection instanceof Promise) {
                $this->listeningConnection = yield $this->listeningConnection;
            }

            try {
                /** @var Listener $listener */
                $listener = yield $this->listeningConnection->listen($channel);
            } catch (\Throwable $exception) {
                if (--$this->listenerCount === 0) {
                    $connection = $this->listeningConnection;
                    $this->listeningConnection = null;
                    $this->push($connection);
                }
                throw $exception;
            }

            $listener->onDestruct(function () {
                if (--$this->listenerCount === 0) {
                    $connection = $this->listeningConnection;
                    $this->listeningConnection = null;
                    $this->push($connection);
                }
            });

            $this->lastUsedAt = \time();

            return $listener;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return call(function () use ($isolation) {
            /** @var Connection $connection */
            $connection = yield from $this->pop();

            try {
                /** @var Transaction $transaction */
                $transaction = yield $connection->transaction($isolation);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            $transaction->onDestruct(function () use ($connection) {
                $this->push($connection);
            });

            $this->lastUsedAt = \time();

            return $transaction;
        });
    }
}
