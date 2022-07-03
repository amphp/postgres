<?php

namespace Amp\Postgres\Internal;

use Amp\Postgres\Listener;
use Amp\Postgres\Notification;
use Revolt\EventLoop;

/** @internal  */
final class ConnectionListener implements Listener, \IteratorAggregate
{
    /** @var null|\Closure(string):void */
    private ?\Closure $unlisten;

    /**
     * @param \Traversable $source Traversable of notifications on the channel.
     * @param string $channel Channel name.
     * @param \Closure(string):void $unlisten Function invoked to stop listening on the channel.
     */
    public function __construct(
        private readonly \Traversable $source,
        private readonly string $channel,
        \Closure $unlisten,
    ) {
        $this->unlisten = $unlisten;
    }

    public function __destruct()
    {
        $this->unlisten();
    }

    /**
     * @return \Traversable<int, Notification>
     */
    public function getIterator(): \Traversable
    {
        // Using a Generator to keep a reference to $this.
        yield from $this->source;
    }

    /**
     * @return string Channel name.
     */
    public function getChannel(): string
    {
        return $this->channel;
    }

    public function isListening(): bool
    {
        return $this->unlisten !== null;
    }

    /**
     * Unlistens from the channel. No more values will be emitted from this listener.
     *
     * @throws \Error If this method was previously invoked.
     */
    public function unlisten(): void
    {
        if (!$this->unlisten) {
            return;
        }

        EventLoop::queue($this->unlisten, $this->channel);
        $this->unlisten = null;
    }
}
