<?php

namespace Kuabound\RabbitMQ;

use AMQPChannel;
use AMQPChannelException;
use AMQPConnectionException;
use AMQPEnvelopeException;
use AMQPExchange;
use AMQPQueue;
use Illuminate\Support\Facades\Log;

class Consumer
{
    protected Connection $connection;

    protected AMQPExchange $exchange;

    protected AMQPChannel $channel;

    protected AMQPQueue $queue;

    /**
     * @param string|null $exchangeName
     * @param array|string $routingKey
     * @param string $queueName
     */
    public function __construct(array|string $routingKey, string $queueName = '', ?string $exchangeName = null)
    {
        $queueName || $queueName = $routingKey;
        $this->connection = new Connection();

        try {
            $this->channel = new AMQPChannel($this->connection->getConn());
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        }

        // 初始化交换机
        if ($exchangeName) {
            $this->exchange = (new Exchange($this->channel))->declare($exchangeName);
        } else {
            $this->exchange = (new Exchange($this->channel))->getDefaultDirectExchange();
        }

        // 初始化队列
        $this->queue = (new Queue($this->channel))->declare($queueName);

        // 绑定队列
        try {
            if (is_array($routingKey)) {
                foreach ($routingKey as $item) {
                    $this->queue->bind($this->exchange->getName(), $item);
                }
            } else {
                $this->queue->bind($this->exchange->getName(), $routingKey);
            }
        } catch (AMQPChannelException $e) {
            Log::error($e);
            throw new RabbitMQException("通道异常", $e);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        }
    }

    /**
     * @param callable $func
     * @param bool $autoAck
     */
    public function consume(callable $func, $autoAck = false)
    {
        try {
            if ($autoAck) {
                // 自动ACK应答
                $this->queue->consume($func, AMQP_AUTOACK);
            } else {
                $this->queue->consume($func);
            }
        } catch (AMQPChannelException $e) {
            Log::error($e);
            throw new RabbitMQException("通道异常", $e);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        } catch (AMQPEnvelopeException $e) {
            Log::error($e);
            throw new RabbitMQException("投递异常", $e);
        }
    }

    public function __destruct()
    {
        if ($this->connection) {
            $this->connection->disconnect();
        }
    }
}
