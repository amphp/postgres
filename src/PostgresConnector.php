<?php

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;

interface PostgresConnector extends SqlConnector
{
    /**
     * @throws SqlException
     */
    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): Connection;
}
