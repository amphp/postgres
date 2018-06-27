<?php

namespace Amp\Postgres;

use Amp\Promise;

interface Link extends \Amp\Sql\Link {
    /**
     * @param int $isolation
     *
     * @return \Amp\Promise<\Amp\Postgres\Transaction>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Sql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function transaction(int $isolation = Transaction::COMMITTED): Promise;

    /**
     * @param string $channel Channel name.
     *
     * @return \Amp\Promise<\Amp\Postgres\Listener>
     *
     * @throws \Amp\Sql\FailureException If the operation fails due to unexpected condition.
     * @throws \Amp\Sql\ConnectionException If the connection to the database is lost.
     * @throws \Amp\Sql\QueryError If the operation fails due to an error in the query (such as a syntax error).
     */
    public function listen(string $channel): Promise;
}
