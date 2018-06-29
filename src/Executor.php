<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Executor {
    const STATEMENT_NAME_PREFIX = "amp_";

    /**
     * @param string $sql
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Sql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql
     * @param mixed[] $params
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Sql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function execute(string $sql, array $params = []): Promise;

    /**
     * @param string $sql
     *
     * @return Promise<@return PromiseStatement>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Sql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function prepare(string $sql): Promise;

    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     */
    public function notify(string $channel, string $payload = ""): Promise;

    /**
     * Indicates if the connection to the database is still alive.
     *
     * @return bool
     */
    public function isAlive(): bool;

    /**
     * Closes the executor. No further queries may be performed.
     */
    public function close();
}
