<?php

namespace Amp\Postgres\Internal;

class PqStatementStorage extends StatementStorage {
    /** @var \pq\Statement */
    public $statement;
}
