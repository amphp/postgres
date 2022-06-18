<?php

namespace Amp\Postgres;

use Amp\Sql\Result;

interface Handle extends Receiver, Quoter
{
    public const STATEMENT_NAME_PREFIX = "amp_";

    /**
     * Execute the statement with the given name and parameters.
     *
     * @param list<int|string> $params List of statement parameters, indexed starting at 0.
     */
    public function statementExecute(string $name, array $params): Result;

    /**
     * Deallocate the statement with the given name.
     */
    public function statementDeallocate(string $name): void;
}
