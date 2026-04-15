<?php

namespace App\Storage\Redis;

use App\Storage\Request;
use App\Storage\Token;
use Illuminate\Support\Facades\Redis;
use Symfony\Component\HttpKernel\Exception\NotFoundHttpException;
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

        return new Token(json_decode($result, true));
    }

    /**
     * @param Token $token
     * @return int
     */
    public function countRequests(Token $token)
    {
        $requestExpiry = (int)config('app.request_expiry');

        if ($requestExpiry <= 0) {
            return $this->redis->zcard(Request::getIdentifier($token->uuid));
        }

        $minScore = time() - $requestExpiry;

        return $this->redis->zcount(Request::getIdentifier($token->uuid), $minScore, '+inf');
    }

    /**
     * @param Token $token
     * @return Token
     */
    public function store(Token $token)
    {
        $this->redis->set(Token::getIdentifier($token->uuid), json_encode($token->attributes()));

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
