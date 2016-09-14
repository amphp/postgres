<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Interop\Async\Awaitable;

interface Statement {
    /**
     * @param mixed ...$params
     *
     * @return \Interop\Async\Awaitable<\Amp\Postgres\Result>
     */
    public function execute(...$params): Awaitable;
}
