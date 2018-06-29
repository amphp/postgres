<?php

namespace Amp\Postgres;

use Amp\Promise;
use Amp\Sql\Link as SqlLink;

interface Link extends SqlLink
{
    /**
     * @param string $channel Channel name.
     *
     * @return Promise<Listener>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Sql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function listen(string $channel): Promise;
}
