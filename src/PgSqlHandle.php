<?php

namespace Amp\Postgres;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Pipeline\Queue;
use Amp\Sql\Common\CommandResult;
use Amp\Sql\ConnectionException;
use Amp\Sql\QueryError;
use Amp\Sql\Result;
use Amp\Sql\SqlException;
use Amp\Sql\Statement;
use Revolt\EventLoop;
use function Amp\async;

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

    /** @var array<string, array<int, array{string, string}>> */
    private static array $typeCache;

    /** @var \PgSql\Connection PostgreSQL connection handle. */
    private ?\PgSql\Connection $handle;

    /** @var array<int, array{string, string, int}> */
    private readonly array $types;

    private ?DeferredFuture $deferred = null;

    private readonly string $poll;

    private readonly string $await;

    /** @var array<string, Queue> */
    private array $listeners = [];

    /** @var array<string, Internal\StatementStorage<string>> */
    private array $statements = [];

    private int $lastUsedAt;

    /**
     * @param \PgSql\Connection $handle PostgreSQL connection handle.
     * @param resource $socket PostgreSQL connection stream socket.
     * @param string $id Connection identifier for determining which cached type table to use.
     */
    public function __construct(\PgSql\Connection $handle, $socket, string $id)
    {
        $this->handle = $handle;

        $this->lastUsedAt = \time();

        $handle = &$this->handle;
        $lastUsedAt = &$this->lastUsedAt;
        $deferred = &$this->deferred;
        $listeners = &$this->listeners;

        $this->poll = EventLoop::onReadable($socket, static function ($watcher) use (
            &$deferred,
            &$lastUsedAt,
            &$listeners,
            &$handle
        ): void {
            $lastUsedAt = \time();

            try {
                if (\pg_connection_status($handle) !== \PGSQL_CONNECTION_OK) {
                    throw new ConnectionException("The connection closed during the operation");
                }

                if (!\pg_consume_input($handle)) {
                    throw new ConnectionException(\pg_last_error($handle));
                }
            } catch (ConnectionException $exception) {
                $handle = null; // Marks connection as dead.
                EventLoop::disable($watcher);

                foreach ($listeners as $listener) {
                    $listener->error($exception);
                }

                $deferred?->error($exception);
                $deferred = null;

                return;
            }

            while ($result = \pg_get_notify($handle, \PGSQL_ASSOC)) {
                $channel = $result["message"];

                if (!isset($listeners[$channel])) {
                    continue;
                }

                $notification = new Notification($channel, $result["pid"], $result["payload"]);
                $listeners[$channel]->pushAsync($notification)->ignore();
            }

            if ($deferred === null) {
                return; // No active query, only notification listeners.
            }

            if (\pg_connection_busy($handle)) {
                return;
            }

            $deferred->complete(\pg_get_result($handle));
            $deferred = null;

            if (empty($listeners)) {
                EventLoop::unreference($watcher);
            }
        });

        $this->await = EventLoop::onWritable($socket, static function ($watcher) use (
            &$deferred,
            &$listeners,
            &$handle
        ): void {
            $flush = \pg_flush($handle);
            if ($flush === 0) {
                return; // Not finished sending data, listen again.
            }

            EventLoop::disable($watcher);

            if ($flush === false) {
                $exception = new ConnectionException(\pg_last_error($handle));
                $handle = null; // Marks connection as dead.

                foreach ($listeners as $listener) {
                    $listener->error($exception);
                }

                $deferred?->error($exception);
                $deferred = null;
            }
        });

        EventLoop::unreference($this->poll);
        EventLoop::disable($this->await);

        /** @psalm-suppress PropertyTypeCoercion */
        $this->types = (self::$typeCache[$id] ??= $this->fetchTypes());
    }

    /**
     * Frees Io watchers from loop.
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * @return array<int, array{string, string, int}>
     *
     * @psalm-suppress LessSpecificReturnStatement, MoreSpecificReturnType
     */
    private function fetchTypes(): array
    {
        \assert($this->handle !== null);

        $result = \pg_query($this->handle, "SELECT t.oid, t.typcategory, t.typdelim, t.typelem
             FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace=n.oid
             WHERE t.typisdefined AND n.nspname IN ('pg_catalog', 'public')");

        $types = [];
        while ($row = \pg_fetch_array($result, null, \PGSQL_NUM)) {
            [$oid, $type, $delimiter, $element] = $row;
            $types[(int) $oid] = [$type, $delimiter, (int) $element];
        }

        return $types;
    }

    public function close(): void
    {
        $this->handle = null;

        $this->deferred?->error(new ConnectionException("The connection was closed"));
        $this->deferred = null;

        EventLoop::cancel($this->poll);
        EventLoop::cancel($this->await);
    }

    public function isAlive(): bool
    {
        return $this->handle instanceof \PgSql\Connection;
    }

    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * @param \Closure $function Function to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return \PgSql\Result
     *
     * @throws SqlException
     */
    private function send(\Closure $function, mixed ...$args): mixed
    {
        while ($this->deferred) {
            try {
                $this->deferred->getFuture()->await();
            } catch (\Throwable) {
                // Ignore failure from another operation.
            }
        }

        if ($this->handle === null) {
            throw new ConnectionException("The connection to the database has been closed");
        }

        while ($result = \pg_get_result($this->handle)) {
            \pg_free_result($result);
        }

        $result = $function($this->handle, ...$args);

        if ($result === false) {
            throw new SqlException(\pg_last_error($this->handle));
        }

        $this->deferred = new DeferredFuture;

        EventLoop::reference($this->poll);
        if ($result === 0) {
            EventLoop::enable($this->await);
        }

        return $this->deferred->getFuture()->await();
    }

    /**
     * @param \PgSql\Result $result PostgreSQL result resource.
     * @param string $sql Query SQL.
     *
     * @return Result
     *
     * @throws SqlException
     * @throws QueryError
     */
    private function createResult(\PgSql\Result $result, string $sql)
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
            case \PGSQL_EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case \PGSQL_COMMAND_OK:
                return new CommandResult(\pg_affected_rows($result), Future::complete($this->fetchNextResult($sql)));

            case \PGSQL_TUPLES_OK:
                return new PgSqlResultSet($result, $this->types, Future::complete($this->fetchNextResult($sql)));

            case \PGSQL_NONFATAL_ERROR:
            case \PGSQL_FATAL_ERROR:
                $diagnostics = [];
                foreach (self::DIAGNOSTIC_CODES as $fieldCode => $description) {
                    $diagnostics[$description] = \pg_result_error_field($result, $fieldCode);
                }
                $message = \pg_result_error($result);
                while (\pg_connection_busy($this->handle) && \pg_get_result($this->handle));
                throw new QueryExecutionError($message, $diagnostics, $sql);

                // no break
            case \PGSQL_BAD_RESPONSE:
                $this->close();
                throw new SqlException(\pg_result_error($result));

            default:
                // @codeCoverageIgnoreStart
                $this->close();
                throw new SqlException("Unknown result status");
                // @codeCoverageIgnoreEnd
        }
    }

    /**
     * @throws SqlException
     */
    private function fetchNextResult(string $sql): ?Result
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        if ($result = \pg_get_result($this->handle)) {
            return $this->createResult($result, $sql);
        }

        return null;
    }

    public function statementExecute(string $name, array $params): Result
    {
        \assert(isset($this->statements[$name]), "Named statement not found when executing");
        return $this->createResult($this->send(\pg_send_execute(...), $name, $params), $this->statements[$name]->sql);
    }

    /**
     * @throws \Error
     */
    public function statementDeallocate(string $name): void
    {
        if (!$this->isAlive()) {
            return; // Connection closed, no need to deallocate.
        }

        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->refCount) {
            return;
        }

        $future = $storage->future;
        $storage->future = async(function () use ($future, $storage, $name): void {
            if (!$future->await()) {
                return; // Statement already deallocated.
            }

            $this->query(\sprintf("DEALLOCATE %s", $name));
            unset($this->statements[$name]);
        });
        $storage->future->ignore();
    }

    public function query(string $sql): Result
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        return $this->createResult($this->send(\pg_send_query(...), $sql), $sql);
    }

    public function execute(string $sql, array $params = []): Result
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        $sql = Internal\parseNamedParams($sql, $names);
        $params = Internal\replaceNamedParams($params, $names);

        return $this->createResult($this->send(\pg_send_query_params(...), $sql, $params), $sql);
    }

    public function prepare(string $sql): Statement
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        $modifiedSql = Internal\parseNamedParams($sql, $names);

        $name = Handle::STATEMENT_NAME_PREFIX . \sha1($modifiedSql);

        while (isset($this->statements[$name])) {
            $storage = $this->statements[$name];

            ++$storage->refCount;
            // Do not return promised prepared statement object, as the $names array may differ.
            $result = $storage->future->await();

            if ($result) { // Null returned if future was from deallocation.
                return new PgSqlStatement($this, $name, $sql, $names);
            }
        }

        $future = async(function () use ($name, $modifiedSql, $sql): string {
            $result = $this->send(\pg_send_prepare(...), $name, $modifiedSql);

            switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
                case \PGSQL_COMMAND_OK:
                    return $name; // Statement created successfully.

                case \PGSQL_NONFATAL_ERROR:
                case \PGSQL_FATAL_ERROR:
                    $diagnostics = [];
                    foreach (self::DIAGNOSTIC_CODES as $fieldCode => $description) {
                        $diagnostics[$description] = \pg_result_error_field($result, $fieldCode);
                    }
                    throw new QueryExecutionError(\pg_result_error($result), $diagnostics, $sql);

                case \PGSQL_BAD_RESPONSE:
                    throw new SqlException(\pg_result_error($result));

                default:
                    // @codeCoverageIgnoreStart
                    throw new SqlException("Unknown result status");
                // @codeCoverageIgnoreEnd
            }
        });

        $storage = new Internal\StatementStorage($sql, $future);
        $this->statements[$name] = $storage;

        try {
            $storage->future->await();
        } catch (\Throwable $exception) {
            unset($this->statements[$name]);
            throw $exception;
        }

        return new PgSqlStatement($this, $name, $sql, $names);
    }

    public function notify(string $channel, string $payload = ""): Result
    {
        if ($payload === "") {
            return $this->query(\sprintf("NOTIFY %s", $this->quoteName($channel)));
        }

        return $this->query(\sprintf("NOTIFY %s, %s", $this->quoteName($channel), $this->quoteString($payload)));
    }

    public function listen(string $channel): Listener
    {
        if (isset($this->listeners[$channel])) {
            throw new QueryError(\sprintf("Already listening on channel '%s'", $channel));
        }

        $this->listeners[$channel] = $source = new Queue();

        try {
            $this->query(\sprintf("LISTEN %s", $this->quoteName($channel)));
        } catch (\Throwable $exception) {
            unset($this->listeners[$channel]);
            throw $exception;
        }

        EventLoop::enable($this->poll);
        return new ConnectionListener($source->iterate(), $channel, $this->unlisten(...));
    }

    /**
     * @throws \Error
     */
    private function unlisten(string $channel): void
    {
        if (!isset($this->listeners[$channel])) {
            return;
        }

        $source = $this->listeners[$channel];
        unset($this->listeners[$channel]);

        if ($this->handle === null) {
            $source->complete();
            return; // Connection already closed.
        }

        try {
            $this->query(\sprintf("UNLISTEN %s", $this->quoteName($channel)));
            $source->complete();
        } catch (\Throwable $exception) {
            $source->error($exception);
        }
    }

    public function quoteString(string $data): string
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_literal($this->handle, $data);
    }

    public function quoteName(string $name): string
    {
        if ($this->handle === null) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_identifier($this->handle, $name);
    }
}
