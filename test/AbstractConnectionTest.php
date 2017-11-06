<?php

namespace Amp\Postgres\Test;

abstract class AbstractConnectionTest extends AbstractLinkTest {
    public function testIsAlive() {
        $this->assertTrue($this->connection->isAlive());
    }
}
