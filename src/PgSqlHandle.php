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
use function Amp\async;
use function Amp\await;

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

    private ?Deferred $deferred = null;

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
            $deferred = null;

            if (empty($listeners)) {
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
     * @inheritDoc
     */
    public function close(): void
    {
        if ($this->deferred) {
            $this->deferred->fail(new ConnectionException("The connection was closed"));
            $this->deferred = null;
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
     * @inheritDoc
     */
    public function isAlive(): bool
    {
        return \is_resource($this->handle);
    }

    /**
     * @inheritDoc
     */
    public function getLastUsedAt(): int
    {
        return $this->lastUsedAt;
    }

    /**
     * @param callable $function Function name to execute.
     * @param mixed ...$args Arguments to pass to function.
     *
     * @return resource
     *
     * @throws FailureException
     */
    private function send(callable $function, ...$args)
    {
        while ($this->deferred) {
            try {
                await($this->deferred->promise());
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

        return await($this->deferred->promise());
    }

    /**
     * @param resource $result PostgreSQL result resource.
     * @param string $sql Query SQL.
     *
     * @return Result
     *
     * @throws FailureException
     * @throws QueryError
     */
    private function createResult($result, string $sql): Result
    {
        switch (\pg_result_status($result, \PGSQL_STATUS_LONG)) {
            case \PGSQL_EMPTY_QUERY:
                throw new QueryError("Empty query string");

            case \PGSQL_COMMAND_OK:
                return new CommandResult(\pg_affected_rows($result), new Success($this->fetchNextResult($sql)));

            case \PGSQL_TUPLES_OK:
                return new PgSqlResultSet($result, new Success($this->fetchNextResult($sql)));

            case \PGSQL_NONFATAL_ERROR:
            case \PGSQL_FATAL_ERROR:
                $diagnostics = [];
                foreach (self::DIAGNOSTIC_CODES as $fieldCode => $description) {
                    $diagnostics[$description] = \pg_result_error_field($result, $fieldCode);
                }
                $message = \pg_result_error($result);
                while (\pg_connection_busy($this->handle) && \pg_get_result($this->handle));
                throw new QueryExecutionError($message, $diagnostics, $sql);

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
     * @param string $sql
     *
     * @return Result|null
     *
     * @throws FailureException
     */
    private function fetchNextResult(string $sql): ?Result
    {
        if ($result = \pg_get_result($this->handle)) {
            return $this->createResult($result, $sql);
        }

        return null;
    }

    /**
     * @param string $name
     * @param array $params
     *
     * @return Result
     */
    public function statementExecute(string $name, array $params): Result
    {
        \assert(isset($this->statements[$name]), "Named statement not found when executing");
        return $this->createResult($this->send("pg_send_execute", $name, $params), $this->statements[$name]->sql);
    }

    /**
     * @param string $name
     *
     * @throws \Error
     */
    public function statementDeallocate(string $name): void
    {
        if (!\is_resource($this->handle)) {
            return; // Connection closed, no need to deallocate.
        }

        \assert(isset($this->statements[$name]), "Named statement not found when deallocating");

        $storage = $this->statements[$name];

        if (--$storage->refCount) {
            return;
        }

        unset($this->statements[$name]);

        $this->query(\sprintf("DEALLOCATE %s", $name));
    }

    /**
     * @inheritDoc
     */
    public function query(string $sql): Result
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return $this->createResult($this->send("pg_send_query", $sql), $sql);
    }

    /**
     * @inheritDoc
     */
    public function execute(string $sql, array $params = []): Result
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        $sql = Internal\parseNamedParams($sql, $names);
        $params = Internal\replaceNamedParams($params, $names);

        return $this->createResult($this->send("pg_send_query_params", $sql, $params), $sql);
    }

    /**
     * @inheritDoc
     */
    public function prepare(string $sql): Statement
    {
        if (!\is_resource($this->handle)) {
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

            return new PgSqlStatement($this, $name, $sql, $names);
        }

        $storage = new class {
            use Struct;
            public int $refCount = 1;
            public ?Promise $promise;
            public string $sql;
        };

        $storage->sql = $sql;

        $this->statements[$name] = $storage;

        try {
            await($storage->promise = async(function () use ($name, $modifiedSql, $sql) {
                $result = $this->send("pg_send_prepare", $name, $modifiedSql);

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
    }

    /**
     * @inheritDoc
     */
    public function notify(string $channel, string $payload = ""): Result
    {
        if ($payload === "") {
            return $this->query(\sprintf("NOTIFY %s", $this->quoteName($channel)));
        }

        return $this->query(\sprintf("NOTIFY %s, %s", $this->quoteName($channel), $this->quoteString($payload)));
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
            $this->query(\sprintf("LISTEN %s", $this->quoteName($channel)));
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
     * @throws \Error
     */
    private function unlisten(string $channel): void
    {
        \assert(isset($this->listeners[$channel]), "Not listening on that channel");

        $source = $this->listeners[$channel];
        unset($this->listeners[$channel]);

        if (!\is_resource($this->handle)) {
            $source->complete();
            return; // Connection already closed.
        }

        try {
            $this->query(\sprintf("UNLISTEN %s", $this->quoteName($channel)));
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
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_literal($this->handle, $data);
    }

    /**
     * @inheritDoc
     */
    public function quoteName(string $name): string
    {
        if (!\is_resource($this->handle)) {
            throw new \Error("The connection to the database has been closed");
        }

        return \pg_escape_identifier($this->handle, $name);
    }
}
