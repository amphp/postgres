<?php

namespace Amp\Postgres;

use Amp\{ Promise, Stream, StreamIterator };

class Listener extends StreamIterator implements Operation {
    use Internal\Operation;
    
    /** @var string */
    private $channel;
    
    /** @var callable */
    private $unlisten;
    
    /**
     * @param \Amp\Stream $stream Stream emitting notificatons on the channel.
     * @param string $channel Channel name.
     * @param callable(string $channel): void $unlisten Function invoked to unlisten from the channel.
     */
    public function __construct(Stream $stream, string $channel, callable $unlisten) {
        parent::__construct($stream);
        $this->channel = $channel;
        $this->unlisten = $unlisten;
    }
    
    /**
     * @return string Channel name.
     */
    public function getChannel(): string {
        return $this->channel;
    }
    
    /**
     * Unlistens from the channel. No more values will be emitted on theis channel.
     *
     * @return \Amp\Promise<\Amp\Postgres\CommandResult>
     */
    public function unlisten(): Promise {
        /** @var \Amp\Promise $promise */
        $promise = ($this->unlisten)($this->channel);
        $promise->onResolve(function () {
            $this->complete();
        });
        return $promise;
    }
}
