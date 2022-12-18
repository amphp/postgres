<?php declare(strict_types=1);

namespace Amp\Postgres;

interface PostgresHandle extends PostgresReceiver, PostgresQuoter
{
    public const STATEMENT_NAME_PREFIX = "amp_";

    /**
     * Execute the statement with the given name and parameters.
     *
     * @param list<int|string> $params List of statement parameters, indexed starting at 0.
     */
    public function statementExecute(string $name, array $params): PostgresResult;

    /**
     * Deallocate the statement with the given name.
     */
    public function statementDeallocate(string $name): void;
}
