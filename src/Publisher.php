<?php

namespace Kuabound\RabbitMQ;

use AMQPChannel;
use AMQPChannelException;
use AMQPConnectionException;
use AMQPExchange;
use AMQPExchangeException;
use AMQPQueue;
use Illuminate\Support\Facades\Log;
use Throwable;

class Publisher
{
    /**
     * @var Connection
     */
    protected Connection $connection;

    /**
     * @var AMQPExchange
     */
    protected AMQPExchange $exchange;

    /**
     * @var AMQPChannel
     */
    protected AMQPChannel $channel;

    /**
     * @param string|null $exchangeName
     */
    public function __construct(?string $exchangeName = null)
    {
        $this->connection = new Connection();

        try {
            $this->channel = new AMQPChannel($this->connection->getConn());
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        }

        if ($exchangeName) {
            $this->exchange = (new Exchange($this->channel))->declare($exchangeName);
        } else {
            $this->exchange = (new Exchange($this->channel))->getDefaultDirectExchange();
        }
    }

    /**
     * 发布队列任务的一个快捷方式，对所有异常静默处理（会记录日志，但不往外抛异常），只返回bool值
     * @param array $data 业务数据
     * @param string $routingKey 路由key
     * @param bool $autoDeclareQueue 自动创建一个以$routingKey为队列名的队列，并与$routingKey绑定（大部分时候都是这么绑定的）
     * @param int $delay 延迟时长（单位：秒）
     * @param string $exchangeName 交换机名，不指定则默认为amq.direct
     * @return bool
     */
    public static function quickPublish(array $data, string $routingKey, bool $autoDeclareQueue = true, int $delay = 0, $exchangeName = null): bool
    {
        try {
            $rabbit = new self($exchangeName);

            if ($autoDeclareQueue) {
                $rabbit->declareQueueAndBindToSameNameRouteKey($routingKey);
            }

            if ($delay) {
                $rabbit->delayPublish($data, $routingKey, $delay);
                Log::debug("向RabbitMQ队列[{$routingKey}]投递了一个延迟[{$delay}]任务:" . json_encode($data));
            } else {
                $rabbit->publish($data, $routingKey);
                Log::debug("向RabbitMQ队列[{$routingKey}]投递了一个任务:" . json_encode($data));
            }

            return true;
        } catch (Throwable $e) {
            Log::error($e);
            return false;
        }
    }

    /**
     * 申明一个队列，并绑定到同名路由key
     * @param string $queueName
     * @throws AMQPChannelException
     * @throws AMQPConnectionException
     */
    public function declareQueueAndBindToSameNameRouteKey(string $queueName): void
    {
        $queue = (new Queue($this->channel))->declare($queueName);
        $queue->bind($this->exchange->getName(), $queueName);
    }

    /**
     * 延迟发布一个任务
     * @param array $data
     * @param string $routingKey
     * @param int $delay 延迟时间（单位：秒）
     * @param int $attempt
     * @param null|string $requestId 请求ID，重试的时候需要传这个
     */
    public function delayPublish(array $data, string $routingKey, int $delay = 0, int $attempt = 1, ?string $requestId = null): void
    {
        //申明一个延迟队列
        $delayQueue = $this->declareDelayQueue($routingKey, $delay * 1000);

        //发布一个任务到死信队列
        $this->publish($data, $delayQueue->getName(), $attempt, $requestId);
    }

    /**
     * 申明一个延迟队列
     * @param string $routingKey
     * @param int $delay 延迟时间（单位：毫秒）
     * @return AMQPQueue
     */
    protected function declareDelayQueue(string $routingKey, int $delay): AMQPQueue
    {
        $queue = (new Queue($this->channel))->declare(
            "enqueue.{$routingKey}.{$delay}.delay",
            [
                'x-message-ttl' => $delay,
                'x-dead-letter-exchange' => $this->exchange->getName(),
                'x-dead-letter-routing-key' => $routingKey,
            ]
        );

        //绑定一下
        try {
            $queue->bind($this->exchange->getName(), $queue->getName());
        } catch (AMQPChannelException $e) {
            Log::error($e);
            throw new RabbitMQException("通道异常", $e);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        }

        return $queue;
    }

    /**
     * 立即发布一个任务
     * @param array $data
     * @param string $routingKey
     * @param int $attempt 当前尝试次数
     * @param null|string $requestId 请求ID，重试的时候需要传这个
     */
    public function publish(array $data, string $routingKey, int $attempt = 1, ?string $requestId = null): void
    {
        try {
            //记录请求ID，方便追踪
            $this->exchange->publish(json_encode($data), $routingKey, AMQP_NOPARAM, [
                'delivery_mode' => AMQP_DELIVERY_MODE_PERSISTENT,
                'headers' => [
                    'x-attempt-count' => $attempt,
                    'x-max-attempts' => 5,
                ],
            ]);
        } catch (AMQPChannelException $e) {
            Log::error($e);
            throw new RabbitMQException("通道异常", $e);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        } catch (AMQPExchangeException $e) {
            Log::error($e);
            throw new RabbitMQException("交换机异常", $e);
        }
    }

    public function __destruct()
    {
        $this->connection->disconnect();
    }
}
