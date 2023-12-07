<?php declare(strict_types=1);

namespace Amp\Postgres;

use Amp\Sql\Link;

/**
 * @extends Link<PostgresResult, PostgresStatement, PostgresTransaction>
 */
interface PostgresLink extends PostgresExecutor, Link
{
    /**
     * @return PostgresTransaction Transaction object specific to this library.
     */
    public function beginTransaction(): PostgresTransaction;
}
