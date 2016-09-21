<?php declare(strict_types = 1);

namespace Amp\Postgres;

use Amp\{ Observable, Observer };
use Interop\Async\Awaitable;

class Listener extends Observer implements Operation {
    use Internal\Operation;
    
    /** @var string */
    private $channel;
    
    /** @var callable */
    private $unlisten;
    
    /**
     * @param \Amp\Observable $observable Observable emitting notificatons on the channel.
     * @param string $channel Channel name.
     * @param callable(string $channel): void $unlisten Function invoked to unlisten from the channel.
     */
    public function __construct(Observable $observable, string $channel, callable $unlisten) {
        parent::__construct($observable);
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
     * @return \Interop\Async\Awaitable<\Amp\Postgres\CommandResult>
     */
    public function unlisten(): Awaitable {
        $awaitable = ($this->unlisten)($this->channel);
        $awaitable->when(function () {
            $this->complete();
        });
        return $awaitable;
    }
}
