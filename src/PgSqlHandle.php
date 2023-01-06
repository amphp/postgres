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

    /** @var array<string, Promise<array<int, array{string, string}>>> */
    private static $typeCache;

    /** @var resource|\PgSql\Connection|null PostgreSQL connection handle. */
    private $handle;

    /** @var Promise<array<int, array{string, string}>> */
    private $types;

    /** @var Deferred|null */
    private $deferred;

    /** @var string */
    private $poll;

    /** @var string */
    private $await;

    /** @var Emitter[] */
    private $listeners = [];

    /** @var array<string, object{refCount: int, promise: Promise<string>, sql: string}> */
    private $statements = [];

    /** @var int */
    private $lastUsedAt;

    /**
     * @param resource $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     * @param string $id Connection identifier for determining which cached type table to use.
     */
    public function __construct($handle, $socket, string $id = '')
    {
        $this->handle = $handle;

        $this->lastUsedAt = \time();

        $handle = &$this->handle;
        $lastUsedAt = &$this->lastUsedAt;
        $deferred = &$this->deferred;
        $listeners = &$this->listeners;

        $this->poll = Loop::onReadable($socket, static function ($watcher) use (&$deferred, &$lastUsedAt, &$listeners, &$handle): void {
            $lastUsedAt = \time();

            if (\pg_connection_status($handle) === \PGSQL_CONNECTION_BAD) {
                $handle = null;

                if ($deferred) {
                    $deferred->fail(new ConnectionException("The connection closed during the operation"));
                }
            }

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
                Loop::unreference($watcher);
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

        Loop::unreference($this->poll);
        Loop::disable($this->await);

        $this->types = $this->fetchTypes($id);
    }

    /**
     * Frees Io watchers from loop.
     */
    public function __destruct()
    {
        $this->free();
    }

    private function fetchTypes(string $id): Promise
    {
        if (isset(self::$typeCache[$id])) {
            return self::$typeCache[$id];
        }

        return self::$typeCache[$id] = call(function (): \Generator {
            $result = yield from $this->send(
                "pg_send_query",
                "SELECT t.oid, t.typcategory, t.typdelim, t.typelem
                 FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace=n.oid
                 WHERE t.typisdefined AND n.nspname IN ('pg_catalog', 'public')"
            );

            $types = [];
            while ($row = \pg_fetch_array($result, null, \PGSQL_NUM)) {
                [$oid, $type, $delimiter, $element] = $row;
                $types[(int) $oid] = [$type, $delimiter, (int) $element];
            }
            return $types;
        });
    }

    /**
     * {@inheritdoc}
     */
    public function close(): void
    {
        if ($this->handle instanceof \PgSql\Connection || \is_resource($this->handle)) {
            \pg_cancel_query($this->handle);
            \pg_close($this->handle);
            $this->handle = null;
        }

        $this->free();
    }

    private function free(): void
    {
        if ($this->deferred) {
            $deferred = $this->deferred;
            $this->deferred = null;
            $deferred->fail(new ConnectionException("The connection was closed"));
        }

        Loop::cancel($this->poll);
        Loop::cancel($this->await);
    }

    /**
     * {@inheritdoc}
     */
    public function isAlive(): bool
    {
        return $this->handle instanceof \PgSql\Connection || \is_resource($this->handle);
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

        if (!$this->isAlive()) {
            throw new ConnectionException("The connection to the database has been closed");
        }

        while ($result = \pg_get_result($this->handle)) {
            \pg_free_result($result);
        }

        $result = $function($this->handle, ...$args);

        if ($result === false) {
            throw new FailureException(\pg_last_error($this->handle));
        }

        $this->deferred = new Deferred;

        Loop::reference($this->poll);
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
     * @param string $sql Query SQL.
     *
     * @return \Amp\Sql\CommandResult|ResultSet
     *
     * @throws FailureException
     * @throws QueryError
     */
    private function createResult($result, string $sql, array $types)
    {
        switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
            case \PGSQL_EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case \PGSQL_COMMAND_OK:
                return new PgSqlCommandResult($result);

            case \PGSQL_TUPLES_OK:
                return new PgSqlResultSet($result, $types);

            case \PGSQL_NONFATAL_ERROR:
            case \PGSQL_FATAL_ERROR:
                $diagnostics = [];
                foreach (self::DIAGNOSTIC_CODES as $fieldCode => $desciption) {
                    $diagnostics[$desciption] = \pg_result_error_field($result, $fieldCode);
                }
                $message = \pg_result_error($result);
                while (\pg_connection_busy($this->handle) && \pg_get_result($this->handle));
                throw new QueryExecutionError($message, $diagnostics, null, $sql);

            case \PGSQL_BAD_RESPONSE:
                $this->close();
                throw new FailureException(\pg_result_error($result));

            default:
                // @codeCoverageIgnoreStart
                $this->close();
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
            \assert(isset($this->statements[$name]), "Named statement not found when executing");
            return $this->createResult(
                yield from $this->send("pg_send_execute", $name, $params),
                $this->statements[$name]->sql,
                yield $this->types
            );
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
        if (!$this->isAlive()) {
            return new Success; // Connection closed, no need to deallocate.
        }

        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->refCount) {
            return new Success;
        }

        return $storage->promise = call(function () use ($storage, $name) {
            yield $this->query(\sprintf("DEALLOCATE %s", $name));
            unset($this->statements[$name]);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function query(string $sql): Promise
    {
        if (!$this->isAlive()) {
            throw new \Error("The connection to the database has been closed");
        }

        return call(function () use ($sql) {
            return $this->createResult(yield from $this->send("pg_send_query", $sql), $sql, yield $this->types);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function execute(string $sql, array $params = []): Promise
    {
        if (!$this->isAlive()) {
            throw new \Error("The connection to the database has been closed");
        }

        $sql = Internal\parseNamedParams($sql, $names);
        $params = Internal\replaceNamedParams($params, $names);

        return call(function () use ($sql, $params) {
            return $this->createResult(
                yield from $this->send("pg_send_query_params", $sql, $params),
                $sql,
                yield $this->types
            );
        });
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Promise
    {
        if (!$this->isAlive()) {
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

                return new PgSqlStatement($this, $name, $sql, $names);
            }

            $storage = new class {
                use Struct;
                public $refCount = 1;
                /** @var Promise<string> */
                public $promise;
                /** @var string */
                public $sql;
            };

            $storage->sql = $sql;

            $this->statements[$name] = $storage;

            try {
                yield ($storage->promise = call(function () use ($name, $modifiedSql, $sql) {
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
                            throw new QueryExecutionError(\pg_result_error($result), $diagnostics, null, $sql);

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

        if (!$this->isAlive()) {
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
        if (!$this->isAlive()) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_literal($this->handle, $data);
    }

    /**
     * {@inheritdoc}
     */
    public function quoteName(string $name): string
    {
        if (!$this->isAlive()) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_identifier($this->handle, $name);
    }
}
