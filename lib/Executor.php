<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Executor {
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
     * @throws \Amp\Postgres\FailureException
     */
    public function execute(string $sql, ...$params): Promise;

    /**
     * @param string $sql
     *
     * @return \Amp\Promise<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function prepare(string $sql): Promise;

    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     */
    public function notify(string $channel, string $payload = ""): Promise;
}
