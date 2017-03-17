<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Statement {
    /**
     * @param mixed ...$params
     *
     * @return \Amp\Promise<\Amp\Postgres\Result>
     */
    public function execute(...$params): Promise;
}
