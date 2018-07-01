<?php

namespace Amp\Postgres;

use Amp\CallableMaker;
use Amp\Coroutine;
use Amp\Deferred;
use Amp\Emitter;
use Amp\Loop;
use Amp\Promise;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Success;
use pq;
use function Amp\call;
use function Amp\coroutine;

final class PqHandle implements Handle
{
    use CallableMaker;

    /** @var \pq\Connection PostgreSQL connection object. */
    private $handle;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var \Amp\Deferred|null */
    private $busy;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var \Amp\Emitter[] */
    private $listeners;

    /** @var @return PromiseInternal\PqStatementStorage[] */
    private $statements = [];

    /** @var callable */
    private $fetch;

    /** @var callable */
    private $unlisten;

    /** @var callable */
    private $release;

    /** @var int */
    private $lastUsedAt;

    /**
     * Connection constructor.
     *
     * @param \pq\Connection $handle
     */
    public function __construct(pq\Connection $handle)
    {
        $this->handle = $handle;
        $this->lastUsedAt = \time();

        $handle = &$this->handle;
        $lastUsedAt = &$this->lastUsedAt;
        $deferred = &$this->deferred;
        $listeners = &$this->listeners;

        $this->poll = Loop::onReadable($this->handle->socket, static function ($watcher) use (&$deferred, &$lastUsedAt, &$listeners, &$handle) {
            $lastUsedAt = \time();

            if ($handle->poll() === pq\Connection::POLLING_FAILED) {
                $exception = new ConnectionException($handle->errorMessage);
                $handle = null; // Marks connection as dead.
                Loop::disable($watcher);

                foreach ($listeners as $listener) {
                    $listener->fail($exception);
                }

                if ($deferred !== null) {
                    $deferred->fail($exception);
                }

                return;
            }

            if ($deferred === null) {
                return; // No active query, only notification listeners.
            }

            if ($handle->busy) {
                return; // Not finished receiving data, poll again.
            }

            $deferred->resolve($handle->getResult());

            if (!$deferred && empty($listeners)) {
                Loop::disable($watcher);
            }
        });

        $this->await = Loop::onWritable($this->handle->socket, static function ($watcher) use (&$deferred, &$listeners, &$handle) {
            try {
                if (!$handle->flush()) {
                    return; // Not finished sending data, continue polling for writability.
                }
            } catch (pq\Exception $exception) {
                $exception = new ConnectionException("Flushing the connection failed", 0, $exception);
                $handle = null; // Marks connection as dead.

                foreach ($listeners as $listener) {
                    $listener->fail($exception);
                }

                if ($deferred !== null) {
                    $deferred->fail($exception);
                }
            }

            Loop::disable($watcher);
        });

        Loop::disable($this->poll);
        Loop::disable($this->await);

        $this->fetch = coroutine($this->callableFromInstanceMethod("fetch"));
        $this->unlisten = $this->callableFromInstanceMethod("unlisten");
        $this->release = $this->callableFromInstanceMethod("release");
    }

    /**
     * Frees Io watchers from loop.
     */
    public function __destruct()
    {
        $this->free();
    }

    /**
     * {@inheritdoc}
     */
    public function isAlive(): bool
    {
        return $this->handle !== null;
    }

    /**
     * {@inheritdoc}
     */
    public function lastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * {@inheritdoc}
     */
    public function close()
    {
        if ($this->deferred) {
            $deferred = $this->deferred;
            $this->deferred = null;
            $deferred->fail(new ConnectionException("The connection was closed"));
        }

        $this->handle = null;

        $this->free();
    }

    private function free()
    {
        Loop::cancel($this->poll);
        Loop::cancel($this->await);
    }

    /**
     * @param callable $method Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Generator
     *
     * @resolve \Amp\Sql\CommandResult|\pq\Statement
     *
     * @throws FailureException
     */
    private function send(callable $method, ...$args): \Generator
    {
        while ($this->busy) {
            try {
                yield $this->busy->promise();
            } catch (\Throwable $exception) {
                // Ignore failure from another operation.
            }
        }

        if (!$this->handle) {
            throw new ConnectionException("The connection to the database has been closed");
        }

        try {
            $handle = $method(...$args);

            $this->deferred = $this->busy = new Deferred;

            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }

            $result = yield $this->deferred->promise();
        } catch (pq\Exception $exception) {
            throw new FailureException($this->handle->errorMessage, 0, $exception);
        } finally {
            $this->deferred = $this->busy = null;
        }

        if (!$result instanceof pq\Result) {
            throw new FailureException("Unknown query result");
        }

        switch ($result->status) {
            case pq\Result::EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case pq\Result::COMMAND_OK:
                if ($handle instanceof pq\Statement) {
                    return $handle; // Will be wrapped into a PqStatement object.
                }

                return new PqCommandResult($result);

            case pq\Result::TUPLES_OK:
                return new PqBufferedResultSet($result);

            case pq\Result::SINGLE_TUPLE:
                $this->busy = new Deferred;
                $result = new PqUnbufferedResultSet($this->fetch, $result);
                $result->onDestruct($this->release);
                return $result;

            case pq\Result::NONFATAL_ERROR:
            case pq\Result::FATAL_ERROR:
                throw new QueryExecutionError($result->errorMessage, $result->diag);

            case pq\Result::BAD_RESPONSE:
                throw new FailureException($result->errorMessage);

            default:
                throw new FailureException("Unknown result status");
        }
    }

    private function fetch(): \Generator
    {
        if (!$this->handle->busy) { // Results buffered.
            $result = $this->handle->getResult();
        } else {
            $this->deferred = new Deferred;

            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }

            try {
                $result = yield $this->deferred->promise();
            } finally {
                $this->deferred = null;
            }
        }

        if (!$result) {
            throw new ConnectionException("Connection closed");
        }

        switch ($result->status) {
            case pq\Result::TUPLES_OK: // End of result set.
                return null;

            case pq\Result::SINGLE_TUPLE:
                return $result;

            default:
                throw new FailureException($result->errorMessage);
        }
    }

    private function release()
    {
        \assert(
            $this->busy instanceof Deferred && $this->busy !== $this->deferred,
            "Connection in invalid state when releasing"
        );

        $deferred = $this->busy;
        $this->busy = null;
        $deferred->resolve();
    }

    /**
     * Executes the named statement using the given parameters.
     *
     * @param string $name
     * @param array $params
     *
     * @return Promise
     * @throws FailureException
     */
    public function statementExecute(string $name, array $params): Promise
    {
        \assert(isset($this->statements[$name]), "Named statement not found when executing");

        $statement = $this->statements[$name]->statement;

        return new Coroutine($this->send([$statement, "execAsync"], $params));
    }

    /**
     * @param string $name
     *
     * @return Promise
     *
     * @throws FailureException
     */
    public function statementDeallocate(string $name): Promise
    {
        if (!$this->handle) {
            return new Success; // Connection dead.
        }

        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->count) {
            return new Success;
        }

        unset($this->statements[$name]);

        return new Coroutine($this->send([$storage->statement, "deallocateAsync"]));
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return new Coroutine($this->send([$this->handle, "execAsync"], $sql));
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $sql = Internal\parseNamedParams($sql, $names);
        $params = Internal\replaceNamedParams($params, $names);

        return new Coroutine($this->send([$this->handle, "execParamsAsync"], $sql, $params));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $modifiedSql = Internal\parseNamedParams($sql, $names);

        $name = self::STATEMENT_NAME_PREFIX . \sha1($modifiedSql);

        if (isset($this->statements[$name])) {
            $storage = $this->statements[$name];
            ++$storage->count;

            if ($storage->promise) {
                return $storage->promise;
            }

            return new Success(new PqStatement($this, $name, $sql, $names));
        }

        $this->statements[$name] = $storage = new Internal\PqStatementStorage;

        $promise = $storage->promise = call(function () use ($storage, $names, $name, $sql, $modifiedSql) {
            $statement = yield from $this->send([$this->handle, "prepareAsync"], $name, $modifiedSql);
            $storage->statement = $statement;
            return new PqStatement($this, $name, $sql, $names);
        });
        $promise->onResolve(function ($exception) use ($storage, $name) {
            if ($exception) {
                unset($this->statements[$name]);
                return;
            }

            $storage->promise = null;
        });
        return $promise;
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        return new Coroutine($this->send([$this->handle, "notifyAsync"], $channel, $payload));
    }

    /**
     * {@inheritdoc}
     */
    public function listen(string $channel): Promise
    {
        return call(function () use ($channel) {
            if (isset($this->listeners[$channel])) {
                throw new QueryError(\sprintf("Already listening on channel '%s'", $channel));
            }

            $this->listeners[$channel] = $emitter = new Emitter;

            try {
                yield from $this->send(
                    [$this->handle, "listenAsync"],
                    $channel,
                    static function (string $channel, string $message, int $pid) use ($emitter) {
                        $notification = new Notification;
                        $notification->channel = $channel;
                        $notification->pid = $pid;
                        $notification->payload = $message;
                        $emitter->emit($notification);
                    }
                );
            } catch (\Throwable $exception) {
                unset($this->listeners[$channel]);
                throw $exception;
            }

            Loop::enable($this->poll);
            return new Listener($emitter->iterate(), $channel, $this->unlisten);
        });
    }

    /**
     * @param string $channel
     *
     * @return Promise
     *
     * @throws \Error
     */
    private function unlisten(string $channel): Promise
    {
        \assert(isset($this->listeners[$channel]), "Not listening on that channel");

        $emitter = $this->listeners[$channel];
        unset($this->listeners[$channel]);

        if (!$this->handle) {
            $promise = new Success; // Connection already closed.
        } else {
            $promise = new Coroutine($this->send([$this->handle, "unlistenAsync"], $channel));
        }

        $promise->onResolve([$emitter, "complete"]);
        return $promise;
    }

    /**
     * {@inheritdoc}
     */
    public function quoteString(string $data): string
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return $this->handle->quote($data);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteName(string $name): string
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return $this->handle->quoteName($name);
    }
}
