<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Cancellation;
use Amp\Sql\SqlConfig;
use Amp\Sql\SqlConnector;
use Amp\Sql\SqlException;

/**
 * @extends SqlConnector<PostgresConfig, PostgresConnection>
 */
interface PostgresConnector extends SqlConnector
{
    /**
     * @throws SqlException
     */
    public function connect(SqlConfig $config, ?Cancellation $cancellation = null): PostgresConnection;
}
