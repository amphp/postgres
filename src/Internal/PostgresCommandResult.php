<?php declare(strict_types=1);

namespace Amp\Postgres\Internal;

use Amp\Future;
use Amp\Postgres\PostgresResult;
use Amp\Sql\Common\CommandResult;

/**
 * @internal
 * @psalm-import-type TFieldType from PostgresResult
 * @implements \IteratorAggregate<int, never>
 */
final class PostgresCommandResult implements PostgresResult, \IteratorAggregate
{
    /** @var CommandResult<TFieldType, PostgresResult> */
    private readonly CommandResult $delegate;

    /**
     * @param Future<PostgresResult|null> $nextResult
     */
    public function __construct(int $affectedRows, Future $nextResult)
    {
        $this->delegate = new CommandResult($affectedRows, $nextResult);
    }

    public function fetchRow(): ?array
    {
        return $this->delegate->fetchRow();
    }

    public function getIterator(): \Traversable
    {
        return $this->delegate->getIterator();
    }

    public function getNextResult(): ?PostgresResult
    {
        return $this->delegate->getNextResult();
    }

    public function getRowCount(): ?int
    {
        return $this->delegate->getRowCount();
    }

    public function getColumnCount(): ?int
    {
        return $this->delegate->getColumnCount();
    }
}
