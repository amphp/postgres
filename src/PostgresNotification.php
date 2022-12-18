<?php declare(strict_types=1);

namespace Amp\Postgres;

final class PostgresNotification
{
    /**
     * @param non-empty-string $channel Channel name.
     * @param positive-int $pid PID of message source.
     * @param string $payload Message payload.
     */
    public function __construct(
        private readonly string $channel,
        private readonly int $pid,
        private readonly string $payload,
    ) {
    }

    /**
     * @return non-empty-string
     */
    public function getChannel(): string
    {
        return $this->channel;
    }

    /**
     * @return positive-int
     */
    public function getPid(): int
    {
        return $this->pid;
    }

    public function getPayload(): string
    {
        return $this->payload;
    }
}
