<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Connector {
    /**
     * @param string $connectionString
     *
     * @return Promise<Connection>
     */
    public function connect(string $connectionString): Promise;
}
