<?php

namespace Amp\Postgres;

use AsyncInterop\Promise;

interface Statement {
    /**
     * @param mixed ...$params
     *
     * @return \AsyncInterop\Promise<\Amp\Postgres\Result>
     */
    public function execute(...$params): Promise;
}
