<?php

namespace Amp\Postgres\Internal;

use Amp\Struct;

class StatementStorage {
    use Struct;

    /** @var \Amp\Promise|null */
    public $promise;

    /** @var int */
    public $count = 1;
}
