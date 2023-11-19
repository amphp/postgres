<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\Link;

/**
 * @extends Link<PostgresResult, PostgresStatement, PostgresTransaction>
 */
interface PostgresLink extends Link, PostgresExecutor
{
}
