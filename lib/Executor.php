<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Awaitable;

interface Executor {
    /**
     * @param string $sql
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function query(string $sql): Awaitable;

    /**
     * @param string $sql
     * @param mixed ...$params
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Result>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function execute(string $sql, ...$params): Awaitable;

    /**
     * @param string $sql
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Statement>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function prepare(string $sql): Awaitable;
    
    /**
     * @param string $channel Channel name.
     * @param string $payload Notification payload.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     */
    public function notify(string $channel, string $payload = ""): Awaitable;
}
