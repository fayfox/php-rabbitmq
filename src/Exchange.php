<?php

namespace Kuabound\RabbitMQ;

use AMQPChannel;
use AMQPChannelException;
use AMQPConnectionException;
use AMQPExchange;
use AMQPExchangeException;
use Illuminate\Support\Facades\Log;

class Exchange
{
    private AMQPChannel $channel;

    public function __construct(AMQPChannel $channel)
    {
        $this->channel = $channel;
    }

    /**
     * 获取默认的Direct交换机
     * @return AMQPExchange
     */
    public function getDefaultDirectExchange(): AMQPExchange
    {
        return $this->declare(env('APP_NAME', 'amq.direct'));
    }

    /**
     * 自定义一个DIRECT交换机
     * @param string $name
     * @param string $type 交换机类型，默认为direct
     * @return AMQPExchange
     */
    public function declare(string $name, string $type = AMQP_EX_TYPE_DIRECT): AMQPExchange
    {
        // 初始化交换机
        try {
            $exchange = new AMQPExchange($this->channel);
        } catch (AMQPConnectionException $e) {
            Log::error($e);
            throw new RabbitMQException("连接异常", $e);
        } catch (AMQPExchangeException $e) {
            Log::error($e);
            throw new RabbitMQException("交换机异常", $e);
        }

        $exchange->setName($name);
        $exchange->setType($type);
        // 持久化
        $exchange->setFlags(AMQP_DURABLE);

        try {
            $exchange->declareExchange();
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

        return $exchange;
    }
}