<?php

namespace Amp\Postgres;

use Amp\Pipeline;

interface Listener extends Pipeline
{
    /**
     * @return Notification|null
     */
    public function continue(): ?Notification;

    /**
     * @return string Channel name.
     */
    public function getChannel(): string;

    /**
     * @return bool
     */
    public function isListening(): bool;

    /**
     * Unlistens from the channel. No more values will be emitted from this listener.
     *
     * @throws \Error If this method was previously invoked.
     */
    public function unlisten(): void;
}
