<?php declare(strict_types = 1);

namespace Amp\Postgres\Internal;

trait Operation {
    /** @var bool */
    private $complete = false;
    
    /** @var callable[] */
    private $onComplete = [];
    
    public function __destruct() {
        $this->complete();
    }
    
    public function onComplete(callable $onComplete) {
        if ($this->complete) {
            $onComplete();
            return;
        }
        
        $this->onComplete[] = $onComplete;
    }
    
    private function complete() {
        if ($this->complete) {
            return;
        }
        
        $this->complete = true;
        foreach ($this->onComplete as $callback) {
            $callback();
        }
        $this->onComplete = null;
    }
}
