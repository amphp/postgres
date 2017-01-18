<?php

namespace Amp\Postgres;

use AsyncInterop\Promise;

interface Executor {
    /**
     * @param string $sql
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql
     * @param mixed ...$params
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function execute(string $sql, ...$params): Promise;

    /**
     * @param string $sql
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function prepare(string $sql): Promise;
    
    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\CommandResult>
     */
    public function notify(string $channel, string $payload = ""): Promise;
}
