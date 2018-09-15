<?php

namespace Amp\Postgres;

use Amp\Sql\ResultSet as SqlResultSet;

interface ResultSet extends SqlResultSet
{
    /**
     * Returns the number of fields (columns) in each row.
     *
     * @return int
     */
    public function getFieldCount(): int;
}
