<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;

interface PostgresConnector extends SqlConnector
{
    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): Connection;
}
