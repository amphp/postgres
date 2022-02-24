<?php

namespace Amp\Postgres;

final class Notification
{
    /**
     * @param string $channel Channel name.
     * @param int $pid PID of message source.
     * @param string $payload Message payload.
     */
    public function __construct(
        public readonly string $channel,
        public readonly int $pid,
        public readonly string $payload,
    ) {
    }
}
