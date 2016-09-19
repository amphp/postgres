<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Awaitable;

interface Connection extends Executor {
    /**
     * @param int $isolation
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Transaction>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Awaitable;
    
    /**
     * @param string $channel Channel name.
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Listener>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function listen(string $channel): Awaitable;
}
