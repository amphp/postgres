<?php

namespace Amp\Postgres;

use Amp\Iterator;
use Amp\Promise;

interface Listener extends Iterator
{
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
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws \Error If this method was previously invoked.
     */
    public function unlisten(): Promise;
}
