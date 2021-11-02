<?php

namespace Amp\Postgres;

use Revolt\EventLoop;

final class ConnectionListener implements Listener, \IteratorAggregate
{
    private \Traversable $source;

    private string $channel;

    /** @var callable|null */
    private $unlisten;

    /**
     * @param \Traversable $source Traversable of notificatons on the channel.
     * @param string $channel Channel name.
     * @param callable(string):void $unlisten Function invoked to unlisten from the channel.
     */
    public function __construct(\Traversable $source, string $channel, callable $unlisten)
    {
        $this->source = $source;
        $this->channel = $channel;
        $this->unlisten = $unlisten;
    }

    public function __destruct()
    {
        if ($this->unlisten) {
            EventLoop::queue($this->unlisten, $this->channel);
        }
    }

    /**
     * @return \Traversable<int, Notification>
     */
    public function getIterator(): \Traversable
    {
        return $this->source;
    }

    /**
     * @return string Channel name.
     */
    public function getChannel(): string
    {
        return $this->channel;
    }

    /**
     * @return bool
     */
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

        $unlisten = $this->unlisten;
        $this->unlisten = null;
        $unlisten($this->channel);
    }
}
