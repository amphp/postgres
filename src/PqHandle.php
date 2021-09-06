<?php

namespace Amp\Postgres;

use Amp\Coroutine;
use Amp\Deferred;
use Amp\Emitter;
use Amp\Loop;
use Amp\Promise;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Sql\QueryError;
use Amp\Struct;
use Amp\Success;
use pq;
use function Amp\call;
use function Amp\coroutine;

final class PqHandle implements Handle
{
    /** @var pq\Connection PostgreSQL connection object. */
    private $handle;

    /** @var Deferred|null */
    private $deferred;

    /** @var Deferred|null */
    private $busy;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var Emitter[] */
    private $listeners = [];

    /** @var array<string, object{refCount: int, promise: Promise<string>, statement: pq\Statement, sql: string}> */
    private $statements = [];

    /** @var int */
    private $lastUsedAt;

    /**
     * Connection constructor.
     *
     * @param pq\Connection $handle
     */
    public function __construct(pq\Connection $handle)
    {
        $this->handle = $handle;
        $this->lastUsedAt = \time();

        $handle = &$this->handle;
        $lastUsedAt = &$this->lastUsedAt;
        $deferred = &$this->deferred;
        $listeners = &$this->listeners;

        $this->poll = Loop::onReadable($this->handle->socket, static function ($watcher) use (&$deferred, &$lastUsedAt, &$listeners, &$handle): void {
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
                Loop::unreference($watcher);
            }
        });

        $this->await = Loop::onWritable($this->handle->socket, static function ($watcher) use (&$deferred, &$listeners, &$handle): void {
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

        Loop::unreference($this->poll);
        Loop::disable($this->await);
    }

    /**
     * Frees Io watchers from loop.
     */
    public function __destruct()
    {
        $this->close();
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
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        if ($this->deferred) {
            $deferred = $this->deferred;
            $this->deferred = null;
            $deferred->fail(new ConnectionException("The connection was closed"));
        }

        $this->handle = null;

        $this->free();
    }

    private function free(): void
    {
        Loop::cancel($this->poll);
        Loop::cancel($this->await);
    }

    /**
     * @param string|null Query SQL or null if not related.
     * @param callable $method Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Generator
     *
     * @resolve \Amp\Sql\CommandResult|\pq\Statement
     *
     * @throws FailureException
     */
    private function send(?string $sql, callable $method, ...$args): \Generator
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
            $this->deferred = $this->busy = new Deferred;

            $handle = $method(...$args);

            Loop::reference($this->poll);
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
                return new PqUnbufferedResultSet(
                    coroutine(\Closure::fromCallable([$this, 'fetch'])),
                    $result,
                    \Closure::fromCallable([$this, 'release'])
                );

            case pq\Result::NONFATAL_ERROR:
            case pq\Result::FATAL_ERROR:
                while ($this->handle->busy && $this->handle->getResult());
                throw new QueryExecutionError($result->errorMessage, $result->diag, null, $sql ?? '');

            case pq\Result::BAD_RESPONSE:
                $this->close();
                throw new FailureException($result->errorMessage);

            default:
                $this->close();
                throw new FailureException("Unknown result status");
        }
    }

    private function fetch(): \Generator
    {
        if (!$this->handle->busy) { // Results buffered.
            $result = $this->handle->getResult();
        } else {
            $this->deferred = new Deferred;

            Loop::reference($this->poll);
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
                $this->close();
                throw new FailureException($result->errorMessage);
        }
    }

    private function release(): void
    {
        \assert(
            $this->busy instanceof Deferred && $this->busy !== $this->deferred,
            "Connection in invalid state when releasing"
        );

        while ($this->handle->busy && $this->handle->getResult());

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

        $storage = $this->statements[$name];

        \assert($storage->statement instanceof pq\Statement, "Statement storage in invalid state");

        return new Coroutine($this->send($storage->sql, [$storage->statement, "execAsync"], $params));
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

        if (--$storage->refCount) {
            return new Success;
        }

        \assert($storage->statement instanceof pq\Statement, "Statement storage in invalid state");

        return $storage->promise = call(function () use ($storage, $name) {
            yield from $this->send(null, [$storage->statement, "deallocateAsync"]);
            unset($this->statements[$name]);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return new Coroutine($this->send($sql, [$this->handle, "execAsync"], $sql));
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

        return new Coroutine($this->send($sql, [$this->handle, "execParamsAsync"], $sql, $params));
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return call(function () use ($sql) {
            $modifiedSql = Internal\parseNamedParams($sql, $names);

            $name = Handle::STATEMENT_NAME_PREFIX . \sha1($modifiedSql);

            while (isset($this->statements[$name])) {
                $storage = $this->statements[$name];

                ++$storage->refCount;

                // Statement may be being allocated or deallocated. Wait to finish, then check for existence again.
                if ($storage->promise instanceof Promise) {
                    // Do not return promised prepared statement object, as the $names array may differ.
                    yield $storage->promise;
                    --$storage->refCount;
                    continue;
                }

                return new PqStatement($this, $name, $sql, $names);
            }

            $storage = new class {
                use Struct;
                public $refCount = 1;
                /** @var Promise<string> */
                public $promise;
                /** @var pq\Statement */
                public $statement;
                /** @var string */
                public $sql;
            };

            $storage->sql = $sql;

            $this->statements[$name] = $storage;

            try {
                $storage->statement = yield (
                    $storage->promise = new Coroutine($this->send($sql, [$this->handle, "prepareAsync"], $name, $modifiedSql))
                );
            } catch (\Throwable $exception) {
                unset($this->statements[$name]);
                throw $exception;
            } finally {
                $storage->promise = null;
            }

            return new PqStatement($this, $name, $sql, $names);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        return new Coroutine($this->send(null, [$this->handle, "notifyAsync"], $channel, $payload));
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
                    null,
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

            Loop::reference($this->poll);
            return new ConnectionListener($emitter->iterate(), $channel, \Closure::fromCallable([$this, 'unlisten']));
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
            $promise = new Coroutine($this->send(null, [$this->handle, "unlistenAsync"], $channel));
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

    /**
     * @return bool True if result sets are buffered in memory, false if unbuffered.
     */
    public function isBufferingResults(): bool
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return !$this->handle->unbuffered;
    }

    /**
     * Sets result sets to be fully buffered in local memory.
     */
    public function shouldBufferResults()
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $this->handle->unbuffered = false;
    }

    /**
     * Sets result sets to be streamed from the database server.
     */
    public function shouldNotBufferResults()
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $this->handle->unbuffered = true;
    }
}
