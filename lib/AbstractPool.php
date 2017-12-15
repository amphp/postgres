<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Promise;
use function Amp\call;

abstract class AbstractPool implements Pool {
    use CallableMaker;

    /** @var \SplQueue */
    private $idle;

    /** @var \SplObjectStorage */
    private $connections;

    /** @var \Amp\Promise|null */
    private $promise;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var \Amp\Postgres\Connection|\Amp\Promise|null Connection used for notification listening. */
    private $listeningConnection;

    /** @var int Number of listeners on listening connection. */
    private $listenerCount = 0;

    /** @var callable */
    private $push;

    /** @var int */
    private $pending = 0;

    /**
     * @return \Amp\Promise<\Amp\Postgres\Connection>
     *
     * @throws \Amp\Postgres\FailureException
     */
    abstract protected function createConnection(): Promise;

    public function __construct() {
        $this->connections = new \SplObjectStorage;
        $this->idle = new \SplQueue;
        $this->push = $this->callableFromInstanceMethod("push");
    }

    /**
     * {@inheritdoc}
     */
    public function extractConnection(): Promise {
        return call(function () {
            $connection = yield from $this->pop();
            $this->connections->detach($connection);
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
     * @param \Amp\Postgres\Connection $connection
     *
     * @throws \Error if the connection is already part of this pool or if the connection is dead.
     */
    protected function addConnection(Connection $connection) {
        if (isset($this->connections[$connection])) {
            throw new \Error("Connection is already a part of this pool");
        }

        if (!$connection->isAlive()) {
            throw new \Error("The connection is dead");
        }

        $this->connections->attach($connection);
        $this->idle->push($connection);

        if ($this->deferred instanceof Deferred) {
            $this->deferred->resolve($connection);
        }
    }

    /**
     * @return \Generator
     *
     * @resolve \Amp\Postgres\Connection
     */
    private function pop(): \Generator {
        while ($this->promise !== null && $this->connections->count() + $this->pending >= $this->getMaxConnections()) {
            yield $this->promise; // Prevent simultaneous connection creation when connection count is at maximum - 1.
        }

        while ($this->idle->isEmpty()) { // While loop to ensure an idle connection is available after promises below are resolved.
            if ($this->connections->count() + $this->pending >= $this->getMaxConnections()) {
                // All possible connections busy, so wait until one becomes available.
                try {
                    $this->deferred = new Deferred;
                    yield $this->promise = $this->deferred->promise(); // May be resolved with defunct connection.
                } finally {
                    $this->deferred = null;
                    $this->promise = null;
                }
            } else {
                // Max connection count has not been reached, so open another connection.
                ++$this->pending;
                try {
                    $connection = yield $this->createConnection();
                } finally {
                    --$this->pending;
                }

                $this->connections->attach($connection);
                return $connection;
            }
        }

        // Shift a connection off the idle queue.
        return $this->idle->shift();
    }

    /**
     * @param \Amp\Postgres\Connection $connection
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
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise {
        return call(function () use ($sql) {
            /** @var \Amp\Postgres\Connection $connection */
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

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise {
        return call(function () use ($sql, $params) {
            /** @var \Amp\Postgres\Connection $connection */
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

            return $result;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise {
        return call(function () use ($sql) {
            /** @var \Amp\Postgres\Connection $connection */
            $connection = yield from $this->pop();

            try {
                /** @var \Amp\Postgres\Statement $statement */
                $statement = yield $connection->prepare($sql);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            $statement->onDestruct(function () use ($connection) {
                $this->push($connection);
            });

            return $statement;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise {
        return call(function () use ($channel, $payload) {
            /** @var \Amp\Postgres\Connection $connection */
            $connection = yield from $this->pop();

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
                /** @var \Amp\Postgres\Listener $listener */
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

            return $listener;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise {
        return call(function () use ($isolation) {
            /** @var \Amp\Postgres\Connection $connection */
            $connection = yield from $this->pop();

            try {
                /** @var \Amp\Postgres\Transaction $transaction */
                $transaction = yield $connection->transaction($isolation);
            } catch (\Throwable $exception) {
                $this->push($connection);
                throw $exception;
            }

            $transaction->onDestruct(function () use ($connection) {
                $this->push($connection);
            });

            return $transaction;
        });
    }
}
