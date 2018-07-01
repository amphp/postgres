<?php

namespace Amp\Postgres;

use Amp\Struct;

final class Notification
{
    use Struct;

    /** @var string Channel name. */
    public $channel;

    /** @var int PID of message source. */
    public $pid;

    /** @var string Message payload */
    public $payload;
}
