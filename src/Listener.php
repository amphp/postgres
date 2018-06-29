<?php

namespace Amp\Postgres;

use Amp\Iterator;
use Amp\Promise;
use Amp\Sql\Operation;

final class Listener implements Iterator, Operation {
    /** @var \Amp\Iterator */
    private $iterator;

    /** @var string */
    private $channel;

    /** @var callable|null */
    private $unlisten;

    /** @var Internal\ReferenceQueue */
    private $queue;

    /**
     * @param \Amp\Iterator $iterator Iterator emitting notificatons on the channel.
     * @param string $channel Channel name.
     * @param callable(string $channel):  $unlisten Function invoked to unlisten from the channel.
     */
    public function __construct(Iterator $iterator, string $channel, callable $unlisten) {
        $this->iterator = $iterator;
        $this->channel = $channel;
        $this->unlisten = $unlisten;
        $this->queue = new Internal\ReferenceQueue;
    }

    public function __destruct() {
        if ($this->unlisten) {
            $this->unlisten(); // Invokes $this->queue->complete().
        }
    }

    /**
     * {@inheritdoc}
     */
    public function onDestruct(callable $onComplete) {
        $this->queue->onDestruct($onComplete);
    }

    /**
     * {@inheritdoc}
     */
    public function advance(): Promise {
        return $this->iterator->advance();
    }

    /**
     * {@inheritdoc}
     *
     * @return Notification
     */
    public function getCurrent(): Notification {
        return $this->iterator->getCurrent();
    }

    /**
     * @return string Channel name.
     */
    public function getChannel(): string {
        return $this->channel;
    }

    /**
     * Unlistens from the channel. No more values will be emitted from this listener.
     *
     * @return Promise<\Amp\Sql\CommandResult>
     *
     * @throws \Error If this method was previously invoked.
     */
    public function unlisten(): Promise {
        if (!$this->unlisten) {
            throw new \Error("Already unlistened on this channel");
        }

        /** @var  $promise */
        $promise = ($this->unlisten)($this->channel);
        $this->unlisten = null;
        $promise->onResolve([$this->queue, "unreference"]);
        return $promise;
    }
}
