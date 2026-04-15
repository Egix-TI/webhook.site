<?php


namespace App\Storage\Redis;

use App\Storage\Request;
use App\Storage\Token;
use Carbon\Carbon;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Redis;
use Symfony\Component\HttpKernel\Exception\NotFoundHttpException;

class RequestStore implements \App\Storage\RequestStore
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
     * @param Token $token
     * @param string $requestId
     * @return Request
     */
    public function find(Token $token, $requestId)
    {
        $this->deleteExpiredRequests($token);

        $result = $this->redis->get(Request::getIdentifier($token->uuid, $requestId));

        if (!$result) {
            throw new NotFoundHttpException('Request not found');
        }

        return new Request(json_decode($result, true));
    }

    /**
     * @param Token $token
     * @param int $page
     * @param int $perPage
     * @param string $sort
     * @return Collection|static
     */
    public function all(Token $token, $page = 1, $perPage = 50, $sorting = 'oldest')
    {
        $this->deleteExpiredRequests($token);

        $requestIds = $sorting === 'newest'
            ? $this->redis->zrevrange(Request::getIdentifier($token->uuid), 0, -1)
            : $this->redis->zrange(Request::getIdentifier($token->uuid), 0, -1);

        if (empty($requestIds)) {
            return collect();
        }

        $requestKeys = array_map(function ($requestId) use ($token) {
            return Request::getIdentifier($token->uuid, $requestId);
        }, $requestIds);

        $requestsByIndex = collect($this->redis->mget($requestKeys));
        $missingRequestIds = collect($requestIds)
            ->zip($requestsByIndex)
            ->filter(function ($pair) {
                return !$pair[1];
            })
            ->pluck(0)
            ->all();

        if (!empty($missingRequestIds)) {
            $this->redis->zrem(Request::getIdentifier($token->uuid), ...$missingRequestIds);
        }

        $requests = $requestsByIndex
            ->filter()
            ->map(function ($request) {
                return json_decode($request);
            });
        
        $requests = $requests->sortBy(
            function ($request) {
                return Carbon::createFromFormat(
                    'Y-m-d H:i:s',
                    $request->created_at
                )->getTimestamp();
            },
            SORT_REGULAR,
            $sorting === 'newest'
        );
        
        return $requests->forPage(
            $page,
            $perPage
        )->values();
    }


    /**
     * @param Token $token
     * @param callable $callback
     * @return void
     */
    public function iterate(Token $token, callable $callback)
    {
        $this->deleteExpiredRequests($token);

        $requestIds = $this->redis->zrange(Request::getIdentifier($token->uuid), 0, -1);

        if (empty($requestIds)) {
            return;
        }

        foreach (array_chunk($requestIds, 200) as $requestIdChunk) {
            $keys = array_map(function ($requestId) use ($token) {
                return Request::getIdentifier($token->uuid, $requestId);
            }, $requestIdChunk);

            foreach ($this->redis->mget($keys) as $serializedRequest) {
                if (!$serializedRequest) {
                    continue;
                }

                $callback(json_decode($serializedRequest));
            }
        }
    }

    /**
     * @param Token $token
     * @param Request $request
     * @return Request
     */
    public function store(Token $token, Request $request)
    {
        $this->deleteExpiredRequests($token);

        $requestKey = Request::getIdentifier($token->uuid, $request->uuid);
        $requestExpiry = (int)config('app.request_expiry');

        if ($requestExpiry > 0) {
            $this->redis->setex($requestKey, $requestExpiry, json_encode($request->attributes()));
        } else {
            $this->redis->set($requestKey, json_encode($request->attributes()));
        }

        $createdAt = Carbon::createFromFormat('Y-m-d H:i:s', $request->created_at)->getTimestamp();
        $this->redis->zadd(Request::getIdentifier($token->uuid), $createdAt, $request->uuid);

        return $request;
    }

    /**
     * @param Token $token
     * @param Request $request
     * @return Request
     */
    public function delete(Token $token, Request $request)
    {
        $this->redis->zrem(Request::getIdentifier($token->uuid), $request->uuid);

        return $this->redis->del(Request::getIdentifier($token->uuid, $request->uuid));
    }

    /**
     * @param Token $token
     * @return Request
     */
    public function deleteByToken(Token $token)
    {
        $requestIds = $this->redis->zrange(Request::getIdentifier($token->uuid), 0, -1);
        $requestKeys = array_map(function ($requestId) use ($token) {
            return Request::getIdentifier($token->uuid, $requestId);
        }, $requestIds);

        if (!empty($requestKeys)) {
            $this->redis->del(...$requestKeys);
        }

        return $this->redis->del(Request::getIdentifier($token->uuid));
    }

    /**
     * Removes expired request ids from the token index and deletes request payloads.
     *
     * @param Token $token
     * @return void
     */
    private function deleteExpiredRequests(Token $token)
    {
        $requestExpiry = (int)config('app.request_expiry');

        if ($requestExpiry <= 0) {
            return;
        }

        $threshold = time() - $requestExpiry;
        $expiredRequestIds = $this->redis->zrangebyscore(Request::getIdentifier($token->uuid), '-inf', $threshold);

        if (empty($expiredRequestIds)) {
            return;
        }

        $expiredRequestKeys = array_map(function ($requestId) use ($token) {
            return Request::getIdentifier($token->uuid, $requestId);
        }, $expiredRequestIds);

        if (!empty($expiredRequestKeys)) {
            $this->redis->del(...$expiredRequestKeys);
        }

        $this->redis->zremrangebyscore(Request::getIdentifier($token->uuid), '-inf', $threshold);
    }

}
