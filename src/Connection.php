<?php

namespace Kuabound\RabbitMQ;

use AMQPConnection;
use AMQPConnectionException;
use Illuminate\Support\Facades\Log;

class Connection
{
    /**
     * @var AMQPConnection
     */
    private AMQPConnection $conn;

    /**
     * @param bool $auto_connect
     */
    public function __construct($auto_connect = true)
    {
        if ($auto_connect) {
            $this->connect();
        }
    }

    /**
     * @return bool
     */
    public function connect(): bool
    {
        $config = [
            'host' => env('RABBITMQ_HOST'),
            'port' => env('RABBITMQ_PORT'),
            'login' => env('RABBITMQ_USERNAME'),
            'password' => env('RABBITMQ_PASSWORD'),
        ];

        if (!$config['host']) {
            throw new RabbitMQException("RabbitMQ服务器参数未配置");
        }

        try {
            $this->conn = new AMQPConnection($config);
            $this->conn->connect();
            return true;
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException('RabbitMQ服务器连接失败', $e);
        }
    }

    /**
     * @return AMQPConnection
     */
    public function getConn()
    {
        return $this->conn;
    }

    public function disconnect()
    {
        if ($this->conn) {
            $this->conn->disconnect();
        }
    }
}
