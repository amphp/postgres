<?php

namespace Amp\Postgres;

use Amp\Pipeline;

final class ConnectionListener implements Listener, \IteratorAggregate
{
    /** @var Pipeline */
    private Pipeline $pipeline;

    /** @var string */
    private string $channel;

    /** @var callable|null */
    private $unlisten;

    /**
     * @param Pipeline $pipeline Pipeline emitting notificatons on the channel.
     * @param string $channel Channel name.
     * @param callable(string $channel):  $unlisten Function invoked to unlisten from the channel.
     */
    public function __construct(Pipeline $pipeline, string $channel, callable $unlisten)
    {
        $this->pipeline = $pipeline;
        $this->channel = $channel;
        $this->unlisten = $unlisten;
    }

    public function __destruct()
    {
        if ($this->unlisten) {
            $this->unlisten(); // Invokes $this->queue->complete().
        }
    }

    /**
     * @inheritDoc
     */
    public function continue(): ?Notification
    {
        return $this->pipeline->continue();
    }

    /**
     * @inheritDoc
     */
    public function dispose(): void
    {
        $this->pipeline->dispose();
        $this->unlisten();
    }

    public function getIterator(): \Iterator
    {
        yield from $this->pipeline;
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
            throw new \Error("Already unlistened on this channel");
        }

        $unlisten = $this->unlisten;
        $this->unlisten = null;
        $unlisten($this->channel);
    }
}
