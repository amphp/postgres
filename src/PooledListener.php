<?php

namespace Amp\Postgres;

use Amp\Promise;

final class PooledListener implements Listener
{
    /** @var Listener */
    private $listener;

    /** @var callable|null */
    private $release;

    public function __construct(Listener $listener, callable $release)
    {
        $this->listener = $listener;
        $this->release = $release;

        if (!$this->listener->isListening()) {
            ($this->release)();
            $this->release = null;
        }
    }

    public function __destruct()
    {
        if ($this->listener->isListening()) {
            $this->unlisten(); // Invokes $this->release callback.
        }
    }

    public function continue(): Promise
    {
        return $this->listener->continue();
    }

    public function dispose(): void
    {
        $this->listener->dispose();
    }

    public function getChannel(): string
    {
        return $this->listener->getChannel();
    }

    public function isListening(): bool
    {
        return $this->listener->isListening();
    }

    public function unlisten(): Promise
    {
        if (!$this->release) {
            throw new \Error("Already unlistened on this channel");
        }

        $promise = $this->listener->unlisten();
        $promise->onResolve($this->release);

        $this->release = null;

        return $promise;
    }
}
