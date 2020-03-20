<?php

namespace Amp\Postgres;

use Amp\Deferred;
use Amp\Emitter;
use Amp\Loop;
use Amp\Promise;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Sql\QueryError;
use Amp\Struct;
use Amp\Success;
use function Amp\call;

final class PgSqlHandle implements Handle
{
    const DIAGNOSTIC_CODES = [
        \PGSQL_DIAG_SEVERITY => "severity",
        \PGSQL_DIAG_SQLSTATE => "sqlstate",
        \PGSQL_DIAG_MESSAGE_PRIMARY => "message_primary",
        \PGSQL_DIAG_MESSAGE_DETAIL => "message_detail",
        \PGSQL_DIAG_MESSAGE_HINT => "message_hint",
        \PGSQL_DIAG_STATEMENT_POSITION => "statement_position",
        \PGSQL_DIAG_INTERNAL_POSITION => "internal_position",
        \PGSQL_DIAG_INTERNAL_QUERY => "internal_query",
        \PGSQL_DIAG_CONTEXT => "context",
        \PGSQL_DIAG_SOURCE_FILE => "source_file",
        \PGSQL_DIAG_SOURCE_LINE => "source_line",
        \PGSQL_DIAG_SOURCE_FUNCTION => "source_function",
    ];

    /** @var resource PostgreSQL connection handle. */
    private $handle;

    /** @var \Amp\Deferred|null */
    private $deferred;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var \Amp\Emitter[] */
    private $listeners = [];

    /** @var Struct[] */
    private $statements = [];

    /** @var int */
    private $lastUsedAt;

    /**
     * Connection constructor.
     *
     * @param resource $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     */
    public function __construct($handle, $socket)
    {
        $this->handle = $handle;

        $this->lastUsedAt = \time();

        $handle = &$this->handle;
        $lastUsedAt = &$this->lastUsedAt;
        $deferred = &$this->deferred;
        $listeners = &$this->listeners;

        $this->poll = Loop::onReadable($socket, static function ($watcher) use (&$deferred, &$lastUsedAt, &$listeners, &$handle): void {
            $lastUsedAt = \time();

            if (!\pg_consume_input($handle)) {
                $handle = null; // Marks connection as dead.
                Loop::disable($watcher);

                $exception = new ConnectionException(\pg_last_error($handle));

                foreach ($listeners as $listener) {
                    $listener->fail($exception);
                }

                if ($deferred !== null) {
                    $deferred->fail($exception);
                }

                return;
            }

            while ($result = \pg_get_notify($handle, \PGSQL_ASSOC)) {
                $channel = $result["message"];

                if (!isset($listeners[$channel])) {
                    continue;
                }

                $notification = new Notification;
                $notification->channel = $channel;
                $notification->pid = $result["pid"];
                $notification->payload = $result["payload"];
                $listeners[$channel]->emit($notification);
            }

            if ($deferred === null) {
                return; // No active query, only notification listeners.
            }

            if (\pg_connection_busy($handle)) {
                return;
            }

            $deferred->resolve(\pg_get_result($handle));

            if (!$deferred && empty($listeners)) {
                Loop::disable($watcher);
            }
        });

        $this->await = Loop::onWritable($socket, static function ($watcher) use (&$deferred, &$listeners, &$handle): void {
            $flush = \pg_flush($handle);
            if ($flush === 0) {
                return; // Not finished sending data, listen again.
            }

            Loop::disable($watcher);

            if ($flush === false) {
                $exception = new ConnectionException(\pg_last_error($handle));
                $handle = null; // Marks connection as dead.

                foreach ($listeners as $listener) {
                    $listener->fail($exception);
                }

                if ($deferred !== null) {
                    $deferred->fail($exception);
                }
            }
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
     * {@inheritdoc}
     */
    public function close(): void
    {
        if ($this->deferred) {
            $deferred = $this->deferred;
            $this->deferred = null;
            $deferred->fail(new ConnectionException("The connection was closed"));
        }

        $this->free();

        $this->handle = null;
    }

    private function free(): void
    {
        if (\is_resource($this->handle)) {
            \pg_close($this->handle);
        }

        Loop::cancel($this->poll);
        Loop::cancel($this->await);
    }

    /**
     * {@inheritdoc}
     */
    public function isAlive(): bool
    {
        return \is_resource($this->handle);
    }

    /**
     * {@inheritdoc}
     */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * @param callable $function Function name to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \Generator
     *
     * @resolve resource
     *
     * @throws FailureException
     */
    private function send(callable $function, ...$args): \Generator
    {
        while ($this->deferred) {
            try {
                yield $this->deferred->promise();
            } catch (\Throwable $exception) {
                // Ignore failure from another operation.
            }
        }

        if (!\is_resource($this->handle)) {
            throw new ConnectionException("The connection to the database has been closed");
        }

        $result = $function($this->handle, ...$args);

        if ($result === false) {
            throw new FailureException(\pg_last_error($this->handle));
        }

        $this->deferred = new Deferred;

        Loop::enable($this->poll);
        if (0 === $result) {
            Loop::enable($this->await);
        }

        try {
            $result = yield $this->deferred->promise();
        } finally {
            $this->deferred = null;
        }

        return $result;
    }

    /**
     * @param resource $result PostgreSQL result resource.
     * @param string $sql SQL query text or statement id for error message
     *
     * @return \Amp\Sql\CommandResult|ResultSet
     *
     * @throws FailureException
     * @throws QueryError
     */
    private function createResult($result, &$sql)
    {
        switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
            case \PGSQL_EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case \PGSQL_COMMAND_OK:
                return new PgSqlCommandResult($result);

            case \PGSQL_TUPLES_OK:
                return new PgSqlResultSet($result);

            case \PGSQL_NONFATAL_ERROR:
            case \PGSQL_FATAL_ERROR:
                $diagnostics = [];
                foreach (self::DIAGNOSTIC_CODES as $fieldCode => $desciption) {
                    $diagnostics[$desciption] = \pg_result_error_field($result, $fieldCode);
                }
                $diagnostics['sql'] = $sql;
                throw new QueryExecutionError(\pg_result_error($result), $diagnostics);

            case \PGSQL_BAD_RESPONSE:
                throw new FailureException(\pg_result_error($result));

            default:
                // @codeCoverageIgnoreStart
                throw new FailureException("Unknown result status");
                // @codeCoverageIgnoreEnd
        }
    }

    /**
     * @param string $name
     * @param array $params
     *
     * @return Promise
     */
    public function statementExecute(string $name, array $params): Promise
    {
        return call(function () use ($name, $params) {
            return $this->createResult(yield from $this->send("pg_send_execute", $name, $params), $name);
        });
    }

    /**
     * @param string $name
     *
     * @return Promise
     *
     * @throws \Error
     */
    public function statementDeallocate(string $name): Promise
    {
        if (!\is_resource($this->handle)) {
            return new Success; // Connection closed, no need to deallocate.
        }

        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->refCount) {
            return new Success;
        }

        unset($this->statements[$name]);

        return $this->query(\sprintf("DEALLOCATE %s", $name));
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return call(function () use ($sql) {
            return $this->createResult(yield from $this->send("pg_send_query", $sql), $sql);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        $sql = Internal\parseNamedParams($sql, $names);
        $params = Internal\replaceNamedParams($params, $names);

        return call(function () use ($sql, $params) {
            return $this->createResult(yield from $this->send("pg_send_query_params", $sql, $params), $sql);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return call(function () use ($sql) {
            $modifiedSql = Internal\parseNamedParams($sql, $names);

            $name = Handle::STATEMENT_NAME_PREFIX . \sha1($modifiedSql);

            if (isset($this->statements[$name])) {
                $storage = $this->statements[$name];

                ++$storage->refCount;

                if ($storage->promise instanceof Promise) {
                    // Do not return promised prepared statement object, as the $names array may differ.
                    yield $storage->promise;
                }

                return new PgSqlStatement($this, $name, $sql, $names);
            }

            $storage = new class {
                use Struct;
                public $refCount = 1;
                public $promise;
            };

            $this->statements[$name] = $storage;

            try {
                yield ($storage->promise = call(function () use ($name, $modifiedSql) {
                    $result = yield from $this->send("pg_send_prepare", $name, $modifiedSql);

                    switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
                        case \PGSQL_COMMAND_OK:
                            return $name; // Statement created successfully.

                        case \PGSQL_NONFATAL_ERROR:
                        case \PGSQL_FATAL_ERROR:
                            $diagnostics = [];
                            foreach (self::DIAGNOSTIC_CODES as $fieldCode => $description) {
                                $diagnostics[$description] = \pg_result_error_field($result, $fieldCode);
                            }
                            throw new QueryExecutionError(\pg_result_error($result), $diagnostics);

                        case \PGSQL_BAD_RESPONSE:
                            throw new FailureException(\pg_result_error($result));

                        default:
                            // @codeCoverageIgnoreStart
                            throw new FailureException("Unknown result status");
                            // @codeCoverageIgnoreEnd
                    }
                }));
            } catch (\Throwable $exception) {
                unset($this->statements[$name]);
                throw $exception;
            } finally {
                $storage->promise = null;
            }

            return new PgSqlStatement($this, $name, $sql, $names);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function notify(string $channel, string $payload = ""): Promise
    {
        if ($payload === "") {
            return $this->query(\sprintf("NOTIFY %s", $this->quoteName($channel)));
        }

        return $this->query(\sprintf("NOTIFY %s, %s", $this->quoteName($channel), $this->quoteString($payload)));
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
                yield $this->query(\sprintf("LISTEN %s", $this->quoteName($channel)));
            } catch (\Throwable $exception) {
                unset($this->listeners[$channel]);
                throw $exception;
            }

            Loop::enable($this->poll);
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

        if (!\is_resource($this->handle)) {
            $promise = new Success; // Connection already closed.
        } else {
            $promise = $this->query(\sprintf("UNLISTEN %s", $this->quoteName($channel)));
        }

        $promise->onResolve([$emitter, "complete"]);
        return $promise;
    }

    /**
     * {@inheritdoc}
     */
    public function quoteString(string $data): string
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_literal($this->handle, $data);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteName(string $name): string
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_identifier($this->handle, $name);
    }
}
