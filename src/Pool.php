<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Pool as SqlPool;

interface Pool extends Link, SqlPool
{
    public function notify(string $channel, string $payload = ""): Promise;
}
