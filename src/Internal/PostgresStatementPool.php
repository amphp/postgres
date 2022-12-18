<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Postgres\PostgresStatement;
use Amp\Postgres\PostgresTransaction;
use Amp\Sql\Common\StatementPool as SqlStatementPool;
use Amp\Sql\Result as SqlResult;

/**
 * @internal
 * @extends SqlStatementPool<PostgresResult, PostgresStatement, PostgresTransaction>
 */
final class PostgresStatementPool extends SqlStatementPool implements PostgresStatement
{
    protected function createResult(SqlResult $result, \Closure $release): PostgresResult
    {
        \assert($result instanceof PostgresResult);
        return new PostgresPooledResult($result, $release);
    }

    public function execute(array $params = []): PostgresResult
    {
        return parent::execute($params);
    }
}
