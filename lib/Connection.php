<?php

namespace Amp\Postgres;

use AsyncInterop\Promise;

interface Connection extends Executor {
    /**
     * @param int $isolation
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\Transaction>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise;
    
    /**
     * @param string $channel Channel name.
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\Listener>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function listen(string $channel): Promise;
}
