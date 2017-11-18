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
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Postgres\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql
     * @param mixed[] $params
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult|\Amp\Postgres\TupleResult>
     *
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Postgres\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function execute(string $sql, array $params = []): Promise;

    /**
     * @param string $sql
     *
     * @return \Amp\Promise<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Postgres\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function prepare(string $sql): Promise;

    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     *
     * @throws \Amp\Postgres\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Postgres\ConnectionException If the connection to the database is lost.
     */
    public function notify(string $channel, string $payload = ""): Promise;
}
