<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Promise;

interface Connection extends Executor {
    /**
     * @param int $isolation
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\Transaction>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise;
    
    /**
     * @param string $channel Channel name.
     *
     * @return \Interop\Async\Promise<\Amp\Postgres\Listener>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function listen(string $channel): Promise;
}
