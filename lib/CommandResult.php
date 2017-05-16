<?php

namespace Amp\Postgres;

interface CommandResult {
    /**
     * Returns the number of rows affected by the query.
     *
     * @return int
     */
    public function affectedRows(): int;
}
