<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Promise;

interface Executor {
    /**
     * @param string $sql
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function query(string $sql): Promise;

    /**
     * @param string $sql
     * @param mixed ...$params
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function execute(string $sql, ...$params): Promise;

    /**
     * @param string $sql
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function prepare(string $sql): Promise;
    
    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\CommandResult>
     */
    public function notify(string $channel, string $payload = ""): Promise;
}
