<?php

namespace Kuabound\RabbitMQ;

use AMQPEnvelope;
use AMQPQueue;
use Exception;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * RabbitMQ消费者封装一下
 */
abstract class ConsumerCommand extends Command
{
    /**
     * @var array 失败延迟重试延迟时长（单位：秒）
     */
    protected static array $retryDelay = [
        0,
        5,
        10,
        30,
        60,
    ];

    /**
     * @throws Exception
     */
    final public function handle(): void
    {
        try {
            $consumer = new Consumer($this->getRouteKey(), '', $this->getExchangeName());
            Log::info('开始监听' . $this->getRouteKey());
        } catch (Exception $e) {
            Log::error($e);
            throw $e;
        }

        try {
            $consumer->consume(function (AMQPEnvelope $envelope, AMQPQueue $queue) {
                $message = $envelope->getBody();
                Log::debug('接收到队列消息: ' . $message);
                $data = json_decode($message, true);
                if (!$data) {
                    Log::error('数据异常: ' . $message);
                    $queue->ack($envelope->getDeliveryTag());
                    return;
                }

                $attempt_count = intval($envelope->getHeader('x-attempt-count'));
                try {
                    Log::debug("执行第[{$attempt_count}]次消费");
                    $this->consume($data);
                    $queue->ack($envelope->getDeliveryTag());

                    Log::debug("消息队列处理完成: {$message}");
                } catch (Throwable $e) {
                    //捕获业务异常
                    Log::error('业务异常: ' . $e);

                    $max_attempts = intval($envelope->getHeader('x-max-attempts'));
                    if ($max_attempts && $attempt_count < $max_attempts) {
                        //重试
                        $delay = self::$retryDelay[$attempt_count] ?? end(self::$retryDelay);
                        $publisher = new Publisher($this->getExchangeName());
                        if ($delay) {
                            //延迟重试
                            $publisher->delayPublish(
                                $data,
                                $this->getRouteKey(),
                                $delay,//延迟执行
                                $attempt_count + 1,
                                $envelope->getHeader('x-request-id') ?: null
                            );
                        } else {
                            //立即重试
                            $publisher->publish(
                                $data,
                                $this->getRouteKey(),
                                $attempt_count + 1,
                                $envelope->getHeader('x-request-id') ?: null
                            );
                        }

                        //本次返回应答
                        $queue->ack($envelope->getDeliveryTag());
                    } else {
                        //不试了，也不应答，就挂着等人工解决吧
                        Log::alert('无法完成队列任务，需人工解决');
                    }
                }
            });
        } catch (Exception $e) {
            Log::error($e);
            throw $e;
        }
    }

    /**
     * 获取路由Key
     * @return string
     */
    abstract public function getRouteKey(): string;

    /**
     * 交换机名称，默认为amq.direct
     */
    public function getExchangeName(): ?string
    {
        return null;
    }

    /**
     * 具体业务处理逻辑，在子类中实现
     * @param array $data
     */
    abstract public function consume(array $data);
}
