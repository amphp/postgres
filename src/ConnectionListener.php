<?php

namespace Amp\Postgres;

use Amp\Pipeline;
use Amp\Promise;

final class ConnectionListener implements Listener
{
    /** @var Pipeline */
    private $stream;

    /** @var string */
    private $channel;

    /** @var callable|null */
    private $unlisten;

    /**
     * @param Pipeline $pipeline Pipeline emitting notificatons on the channel.
     * @param string $channel Channel name.
     * @param callable(string $channel):  $unlisten Function invoked to unlisten from the channel.
     */
    public function __construct(Pipeline $pipeline, string $channel, callable $unlisten)
    {
        $this->stream = $pipeline;
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
    public function continue(): Promise
    {
        return $this->stream->continue();
    }

    /**
     * @inheritDoc
     */
    public function dispose()
    {
        $this->stream->dispose();
        $this->unlisten();
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
     * @return Promise<void>
     *
     * @throws \Error If this method was previously invoked.
     */
    public function unlisten(): Promise
    {
        if (!$this->unlisten) {
            throw new \Error("Already unlistened on this channel");
        }

        /** @var Promise $promise */
        $promise = ($this->unlisten)($this->channel);
        $this->unlisten = null;

        return $promise;
    }
}
