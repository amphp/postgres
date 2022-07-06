<?php

namespace Amp\Postgres;

final class PostgresNotification
{
    /**
     * @param string $channel Channel name.
     * @param positive-int $pid PID of message source.
     * @param string $payload Message payload.
     */
    public function __construct(
        private readonly string $channel,
        private readonly int $pid,
        private readonly string $payload,
    ) {
    }

    public function getChannel(): string
    {
        return $this->channel;
    }

    public function getPid(): int
    {
        return $this->pid;
    }

    public function getPayload(): string
    {
        return $this->payload;
    }
}
