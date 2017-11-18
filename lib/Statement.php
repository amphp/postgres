<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Statement extends Operation {
    /**
     * @param mixed[] $params
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult|\Amp\Postgres\TupleResult>
     */
    public function execute(array $params = []): Promise;

    /**
     * @return string The SQL string used to prepare the statement.
     */
    public function getQuery(): string;
}
