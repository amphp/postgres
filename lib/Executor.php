<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Executor {
    const STATEMENT_NAME_PREFIX = "amp_";

    /**
     * @param string $sql
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult|\Amp\Postgres\TupleResult>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql
     * @param mixed ...$params
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult|\Amp\Postgres\TupleResult>
     *
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\QueryError If the operation fails due to an error in the query (such as a syntax error).
     * @throws \Amp\Postgres\PendingOperationError If another operation is currently pending on the connection.
     */
    public function execute(string $sql, ...$params): Promise;

    /**
     * @param string $sql
     *
     * @return \Amp\Promise<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\QueryError If the operation fails due to an error in the query (such as a syntax error).
     * @throws \Amp\Postgres\PendingOperationError If another operation is currently pending on the connection.
     */
    public function prepare(string $sql): Promise;

    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\PendingOperationError If another operation is currently pending on the connection.
     */
    public function notify(string $channel, string $payload = ""): Promise;
}
