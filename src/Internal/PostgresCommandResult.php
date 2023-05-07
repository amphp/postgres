<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Postgres\PostgresResult;
use Amp\Sql\Common\CommandResult;

/**
 * @internal
 * @psalm-import-type TFieldType from PostgresResult
 * @extends CommandResult<TFieldType, PostgresResult>
 */
final class PostgresCommandResult extends CommandResult implements PostgresResult
{
    /**
     * Changes return type to this library's Result type.
     */
    public function getNextResult(): ?PostgresResult
    {
        return parent::getNextResult();
    }
}
