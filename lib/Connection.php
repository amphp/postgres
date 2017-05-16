<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Connection extends Executor {
    /**
     * @param int $isolation
     *
     * @return \Amp\Promise<\Amp\Postgres\Transaction>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise;

    /**
     * @param string $channel Channel name.
     *
     * @return \Amp\Promise<\Amp\Postgres\Listener>
     *
     * @throws \Amp\Postgres\FailureException
     */
    public function listen(string $channel): Promise;
}
