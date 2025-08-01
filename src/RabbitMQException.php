<?php

namespace Kuabound\RabbitMQ;

use Throwable;

class RabbitMQException extends \RuntimeException
{
    public function __construct($message = "", Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}