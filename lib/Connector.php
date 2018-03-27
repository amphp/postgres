<?php

namespace Amp\Postgres;

use Amp\CancellationToken;
use Amp\Promise;

interface Connector {
    /**
     * @param string $connectionString
     * @param CancellationToken|null $token
     *
     * @return Promise<Connection>
     */
    public function connect(string $connectionString, CancellationToken $token = null): Promise;
}
