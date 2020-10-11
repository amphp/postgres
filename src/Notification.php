<?php

namespace Amp\Postgres;

use Amp\Struct;

final class Notification
{
    use Struct;

    /** @var string Channel name. */
    public string $channel;

    /** @var int PID of message source. */
    public int $pid;

    /** @var string Message payload */
    public string $payload;
}
