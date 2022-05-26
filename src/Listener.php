<?php

namespace Amp\Postgres;

/**
 * @extends \Traversable<int, Notification>
 */
interface Listener extends \Traversable
{
    /**
     * @return string Channel name.
     */
    public function getChannel(): string;

    public function isListening(): bool;

    /**
     * Stops listening on the channel. No more values will be emitted from this listener.
     */
    public function unlisten(): void;
}
