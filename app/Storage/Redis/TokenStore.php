<?php

namespace App\Storage\Redis;

use App\Storage\Request;
use App\Storage\Token;
use Illuminate\Support\Facades\Redis;
use Symfony\Component\HttpKernel\Exception\GoneHttpException;

class TokenStore implements \App\Storage\TokenStore
{
    /**
     * @var Redis
     */
    private $redis;

    /**
     * TokenStore constructor.
     */
    public function __construct()
    {
        $this->redis = Redis::connection(config('database.redis.connection'));
    }

    /**
     * @param string $tokenId
     * @return Token
     */
    public function find($tokenId)
    {
        $result = $this->redis->get(Token::getIdentifier($tokenId));

        if (!$result) {
            throw new GoneHttpException('Token not found');
        }

        $expiry = (int)config('app.expiry');

        if ($expiry > 0) {
            $this->redis->expire(Token::getIdentifier($tokenId), $expiry);
        }

        return new Token(json_decode($result, true));
    }

    /**
     * @param Token $token
     * @return int
     */
    public function countRequests(Token $token)
    {
        return $this->redis->hlen(Request::getIdentifier($token->uuid));
    }

    /**
     * @param Token $token
     * @return Token
     */
    public function store(Token $token)
    {
        $expiry = (int)config('app.expiry');

        if ($expiry > 0) {
            $this->redis->setex(Token::getIdentifier($token->uuid), $expiry, json_encode($token->attributes()));
        } else {
            $this->redis->set(Token::getIdentifier($token->uuid), json_encode($token->attributes()));
        }

        return $token;
    }

    /**
     * @param Token $token
     * @return Token
     */
    public function delete(Token $token)
    {
        return $this->redis->del(Token::getIdentifier($token->uuid));
    }
}
