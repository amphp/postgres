<?php

namespace Amp\Postgres;

use Amp\Pipeline;
use Amp\Promise;

interface Listener extends Pipeline
{
    /**
     * @return Promise<Notification|null>
     */
    public function continue(): Promise;

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
     * @return Promise<void>
     *
     * @throws \Error If this method was previously invoked.
     */
    public function unlisten(): Promise;
}
