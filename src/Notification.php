<?php

namespace Amp\Postgres;

final class Notification
{
    /** @var string Channel name. */
    public string $channel;

    /** @var int PID of message source. */
    public int $pid;

    /** @var string Message payload */
    public string $payload;
}
