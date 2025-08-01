<?php

namespace Kuabound\RabbitMQ;

use AMQPChannel;
use AMQPChannelException;
use AMQPConnectionException;
use AMQPQueue;
use AMQPQueueException;
use Illuminate\Support\Facades\Log;

class Queue
{
    private AMQPChannel $channel;


    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    /**
     * 申明一个队列
     * @param string $name 队列名
     * @param array $args
     * @return AMQPQueue
     */
    public function declare(string $name, array $args = []): AMQPQueue
    {
        // 初始化队列
        try {
            $queue = new AMQPQueue($this->channel);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        } catch (AMQPQueueException $e) {
            Log::error($e);
            throw new RabbitMQException("队列异常", $e);
        }

        $queue->setName($name);
        $queue->setFlags(AMQP_DURABLE);

        // 绑定自定义参数
        if ($args) {
            $queue->setArguments($args);
        }

        // 申明队列
        try {
            $queue->declareQueue();
        } catch (AMQPChannelException $e) {
            Log::error($e);
            throw new RabbitMQException("通道异常", $e);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        } catch (AMQPQueueException $e) {
            Log::error($e);
            throw new RabbitMQException("队列", $e);
        }

        return $queue;
    }
}