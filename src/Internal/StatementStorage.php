<?php

namespace Amp\Postgres\Internal;

use Amp\Struct;

class StatementStorage {
    use Struct;

    /** @var |null */
    public $promise;

    /** @var int */
    public $count = 1;
}
