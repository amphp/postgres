<?php

namespace Amp\Postgres;

use Amp\Deferred;
use Amp\Loop;
use Amp\PipelineSource;
use Amp\Promise;
use Amp\Sql\Common\CommandResult;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Sql\QueryError;
use Amp\Sql\Result;
use Amp\Sql\Statement;
use Amp\Struct;
use Amp\Success;
use pq;
use function Amp\async;
use function Amp\asyncCallable;
use function Amp\await;

final class PqHandle implements Handle
{
    private ?pq\Connection $handle;

    private ?Deferred $deferred = null;

    private ?Deferred $busy = null;

    private string $poll;

    private string $await;

    /** @var PipelineSource[] */
    private array $listeners = [];

    /** @var object[] Anonymous class using Struct trait. */
    private array $statements = [];

    private int $lastUsedAt;

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
            $deferred = null;

            if (empty($listeners)) {
                Loop::disable($watcher);
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

        Loop::disable($this->poll);
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
     * @inheritDoc
     */
    public function isAlive(): bool
    {
        return $this->handle !== null;
    }

    /**
     * @inheritDoc
     */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * @inheritDoc
     */
    public function close(): void
    {
        if ($this->deferred) {
            $this->deferred->fail(new ConnectionException("The connection was closed"));
            $this->deferred = null;
        }

        $this->handle = null;

        Loop::cancel($this->poll);
        Loop::cancel($this->await);
    }

    /**
     * @param string|null Query SQL or null if not related.
     * @param callable $method Method to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return Result|pq\Statement
     *
     * @throws FailureException
     */
    private function send(?string $sql, callable $method, ...$args): Result|pq\Statement
    {
        while ($this->busy) {
            try {
                await($this->busy->promise());
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

            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }

            $result = await($this->deferred->promise());
        } catch (pq\Exception $exception) {
            throw new FailureException($this->handle->errorMessage, 0, $exception);
        } finally {
            $this->busy = null;
        }

        if (!$result instanceof pq\Result) {
            throw new FailureException("Unknown query result");
        }

        $result = $this->makeResult($result, $sql);

        if ($handle instanceof pq\Statement) {
            return $handle; // Will be wrapped into a PqStatement object.
        }

        return $result;
    }

    /**
     * @param pq\Result   $result
     * @param string|null $sql
     *
     * @return Result
     *
     * @throws FailureException
     */
    private function makeResult(pq\Result $result, ?string $sql): Result
    {
        switch ($result->status) {
            case pq\Result::EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case pq\Result::COMMAND_OK:
                return new CommandResult($result->affectedRows, new Success($this->fetchNextResult($sql)));

            case pq\Result::TUPLES_OK:
                return new PqBufferedResultSet($result, new Success($this->fetchNextResult($sql)));

            case pq\Result::SINGLE_TUPLE:
                $this->busy = new Deferred;
                return new PqUnbufferedResultSet(
                    asyncCallable(fn() => $this->fetch($sql)),
                    $result,
                    $this->busy->promise()
                );

            case pq\Result::NONFATAL_ERROR:
            case pq\Result::FATAL_ERROR:
                while ($this->handle->busy && $this->handle->getResult());
                throw new QueryExecutionError($result->errorMessage, $result->diag, $sql ?? '');

            case pq\Result::BAD_RESPONSE:
                $this->close();
                throw new FailureException($result->errorMessage);

            default:
                $this->close();
                throw new FailureException("Unknown result status");
        }
    }

    /**
     * @param string|null $sql
     *
     * @return Result|null
     *
     * @throws FailureException
     */
    private function fetchNextResult(?string $sql): ?Result
    {
        if (!$this->handle->busy && ($next = $this->handle->getResult()) instanceof pq\Result) {
            return $this->makeResult($next, $sql);
        }

        return null;
    }

    private function fetch(string $sql): ?pq\Result
    {
        if (!$this->handle) {
            throw new ConnectionException("Connection closed");
        }

        if (!$this->handle->busy) { // Results buffered.
            $result = $this->handle->getResult();
        } else {
            $this->deferred = new Deferred;

            Loop::enable($this->poll);
            if (!$this->handle->flush()) {
                Loop::enable($this->await);
            }

            $result = await($this->deferred->promise());
        }

        if (!$result) {
            throw new ConnectionException("Connection closed");
        }

        switch ($result->status) {
            case pq\Result::TUPLES_OK: // End of result set.
                $deferred = $this->busy;
                $this->busy = null;

                try {
                    $deferred->resolve($this->fetchNextResult($sql));
                } catch (\Throwable $exception) {
                    $deferred->fail($exception);
                }

                return null;

            case pq\Result::SINGLE_TUPLE:
                return $result;

            default:
                $this->close();
                throw new FailureException($result->errorMessage);
        }
    }

    /**
     * Executes the named statement using the given parameters.
     *
     * @param string $name
     * @param array $params
     *
     * @return Result
     * @throws FailureException
     */
    public function statementExecute(string $name, array $params): Result
    {
        \assert(isset($this->statements[$name]), "Named statement not found when executing");

        $storage = $this->statements[$name];

        \assert($storage->statement instanceof pq\Statement, "Statement storage in invalid state");

        return $this->send($storage->sql, [$storage->statement, "execAsync"], $params);
    }

    /**
     * @param string $name

     * @throws FailureException
     */
    public function statementDeallocate(string $name): void
    {
        if (!$this->handle) {
            return; // Connection dead.
        }

        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->refCount) {
            return;
        }

        unset($this->statements[$name]);

        $this->send(null, [$storage->statement, "deallocateAsync"]);
    }

    /**
     * @inheritDoc
     */
    public function query(string $sql): Result
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return $this->send($sql, [$this->handle, "execAsync"], $sql);
    }

    /**
     * @inheritDoc
     */
    public function execute(string $sql, array $params = []): Result
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $sql = Internal\parseNamedParams($sql, $names);
        $params = Internal\replaceNamedParams($params, $names);

        return $this->send($sql, [$this->handle, "execParamsAsync"], $sql, $params);
    }

    /**
     * @inheritDoc
     */
    public function prepare(string $sql): Statement
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $modifiedSql = Internal\parseNamedParams($sql, $names);

        $name = Handle::STATEMENT_NAME_PREFIX . \sha1($modifiedSql);

        if (isset($this->statements[$name])) {
            $storage = $this->statements[$name];

            ++$storage->refCount;

            if ($storage->promise instanceof Promise) {
                // Do not return promised prepared statement object, as the $names array may differ.
                await($storage->promise);
            }

            return new PqStatement($this, $name, $sql, $names);
        }

        $storage = new class {
            use Struct;
            public int $refCount = 1;
            public ?Promise $promise;
            public pq\Statement $statement;
            public string $sql;
        };

        $storage->sql = $sql;

        $this->statements[$name] = $storage;

        try {
            $storage->statement = await(
                $storage->promise = async(fn() => $this->send($sql, [$this->handle, "prepareAsync"], $name, $modifiedSql))
            );
        } catch (\Throwable $exception) {
            unset($this->statements[$name]);
            throw $exception;
        } finally {
            $storage->promise = null;
        }

        return new PqStatement($this, $name, $sql, $names);
    }

    /**
     * @inheritDoc
     */
    public function notify(string $channel, string $payload = ""): Result
    {
        return $this->send(null, [$this->handle, "notifyAsync"], $channel, $payload);
    }

    /**
     * @inheritDoc
     */
    public function listen(string $channel): Listener
    {
        if (isset($this->listeners[$channel])) {
            throw new QueryError(\sprintf("Already listening on channel '%s'", $channel));
        }

        $this->listeners[$channel] = $source = new PipelineSource;

        try {
            $this->send(
                null,
                [$this->handle, "listenAsync"],
                $channel,
                static function (string $channel, string $message, int $pid) use ($source): void {
                    $notification = new Notification;
                    $notification->channel = $channel;
                    $notification->pid = $pid;
                    $notification->payload = $message;
                    $source->emit($notification);
                }
            );
        } catch (\Throwable $exception) {
            unset($this->listeners[$channel]);
            throw $exception;
        }

        Loop::enable($this->poll);
        return new ConnectionListener($source->pipe(), $channel, \Closure::fromCallable([$this, 'unlisten']));
    }

    /**
     * @param string $channel
     *
     * @return Promise
     *
     * @throws \Error
     */
    private function unlisten(string $channel): void
    {
        \assert(isset($this->listeners[$channel]), "Not listening on that channel");

        $source = $this->listeners[$channel];
        unset($this->listeners[$channel]);

        if (!$this->handle) {
            $source->complete();
            return; // Connection already closed.
        }

        try {
            $this->send(null, [$this->handle, "unlistenAsync"], $channel);
            $source->complete();
        } catch (\Throwable $exception) {
            $source->fail($exception);
        }
    }

    /**
     * @inheritDoc
     */
    public function quoteString(string $data): string
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        return $this->handle->quote($data);
    }

    /**
     * @inheritDoc
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
    public function shouldBufferResults(): void
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $this->handle->unbuffered = false;
    }

    /**
     * Sets result sets to be streamed from the database server.
     */
    public function shouldNotBufferResults(): void
    {
        if (!$this->handle) {
            throw new \Error("The connection to the database has been closed");
        }

        $this->handle->unbuffered = true;
    }
}
