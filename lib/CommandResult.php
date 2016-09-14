<?php declare(strict_types = 1);

namespace Amp\Postgres;

interface CommandResult extends \Countable, Result {
    /**
     * Returns the number of rows affected by the query.
     *
     * @return int
     */
    public function affectedRows(): int;
}
