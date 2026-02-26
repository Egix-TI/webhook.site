<?php

namespace App\Http\Controllers;

use App\Events\RequestCreated;
use App\Storage\Request;
use App\Storage\RequestStore;
use App\Storage\Token;
use App\Storage\TokenStore;
use Illuminate\Cache\Repository;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request as HttpRequest;
use Illuminate\Http\Response;
use Symfony\Component\HttpFoundation\StreamedResponse;

class RequestController extends Controller
{
    /**
     * @var Repository
     */
    private $cache;
    /**
     * @var TokenStore
     */
    private $tokens;
    /**
     * @var RequestStore
     */
    private $requests;

    /**
     * RequestController constructor.
     * @param Repository $cache
     * @param TokenStore $tokens
     * @param RequestStore $requests
     */
    public function __construct(Repository $cache, TokenStore $tokens, RequestStore $requests)
    {
        $this->cache = $cache;
        $this->tokens = $tokens;
        $this->requests = $requests;
    }

    /**
     * @param HttpRequest $httpRequest
     * @return Response
     */
    public function create(HttpRequest $httpRequest, $tokenId)
    {
        $token = $this->tokens->find($tokenId);

        $this->guardOverQuota($token);

        if ($token->timeout) {
            sleep($token->timeout);
        }

        $request = Request::createFromRequest($httpRequest);

        $this->requests->store($token, $request);

        broadcast(new RequestCreated($token, $request));

        $responseStatus = preg_match('/[1-5][0-9][0-9]/', $httpRequest->segment(2))
            ? $httpRequest->segment(2)
            : $token->default_status;

        $response = new Response(
            $token->default_content,
            $responseStatus,
            [
                'Content-Type' => $token->default_content_type,
                'X-Request-Id' => $request->uuid,
                'X-Token-Id' => $token->uuid,
            ]
        );

        if ($token->cors) {
            $response->withHeaders($this::corsHeaders());
        }

        return $response;
    }

    /**
     * @param Token $token
     * @return void
     */
    private function guardOverQuota($token)
    {
        if ($this->tokens->countRequests($token) >= config('app.max_requests')) {
            abort(Response::HTTP_GONE, 'Too many requests, please create a new URL/token');
        }
    }

    /**
     * @param HttpRequest $httpRequest
     * @param string $tokenId
     * @return JsonResponse
     */
    public function all(HttpRequest $httpRequest, $tokenId)
    {
        $token = $this->tokens->find($tokenId);
        $sorting = $httpRequest->get('sorting', 'oldest');
        $page = (int)$httpRequest->get('page', 1);
        $perPage = (int)$httpRequest->get('per_page', 50);
        $requests = $this->requests->all($token, $page, $perPage, $sorting);
        $total = $this->tokens->countRequests($token);

        return new JsonResponse([
            'data' => $requests,
            'total' => $total,
            'per_page' => $perPage,
            'current_page' => $page,
            'is_last_page' => ($requests->count() + (($page - 1) * $perPage)) >= $total,
            'from' => (($page - 1) * $perPage) + 1,
            'to' => min($total, $requests->count() + (($page - 1) * $perPage)),
        ]);
    }

    /**
     * @param $tokenId
     * @param $requestId
     * @return mixed
     */
    public function find($tokenId, $requestId)
    {
        $token = $this->tokens->find($tokenId);
        $request = $this->requests->find($token, $requestId);

        return new JsonResponse($request);
    }


    /**
     * @param string $tokenId
     * @return StreamedResponse
     */
    public function exportCsv($tokenId)
    {
        $token = $this->tokens->find($tokenId);

        $filename = sprintf('webhook-site-%s-requests.csv', $token->uuid);

        $response = new StreamedResponse(function () use ($token) {
            $output = fopen('php://output', 'w');

            fputcsv($output, [
                'uuid',
                'token_id',
                'created_at',
                'method',
                'url',
                'ip',
                'hostname',
                'user_agent',
                'query',
                'headers',
                'request',
                'content',
            ]);

            $this->requests->iterate($token, function ($request) use ($output) {
                fputcsv($output, [
                    $request->uuid,
                    $request->token_id,
                    $request->created_at,
                    $request->method,
                    $request->url,
                    $request->ip,
                    $request->hostname,
                    $request->user_agent,
                    $this->toJsonString($request->query),
                    $this->toJsonString($request->headers),
                    $this->toJsonString(isset($request->request) ? $request->request : null),
                    isset($request->content) ? $request->content : null,
                ]);
            });

            fclose($output);
        });

        $response->headers->set('Content-Type', 'text/csv; charset=UTF-8');
        $response->headers->set('Content-Disposition', sprintf('attachment; filename="%s"', $filename));

        return $response;
    }

    /**
     * @param mixed $value
     * @return string|null
     */
    private function toJsonString($value)
    {
        if ($value === null) {
            return null;
        }

        return json_encode($value);
    }

    /**
     * @param string $tokenId
     * @param string $requestId
     * @return Response
     */
    public function raw($tokenId, $requestId)
    {
        $token = $this->tokens->find($tokenId);
        $request = $this->requests->find($token, $requestId);

        $contentType = $request->isJson() ? 'application/json' : 'text/plain';

        return new Response($request->content, Response::HTTP_OK, ['content-type' => $contentType]);
    }

    /**
     * @param string $tokenId
     * @param string $requestId
     * @return JsonResponse
     */
    public function delete($tokenId, $requestId)
    {
        $token = $this->tokens->find($tokenId);
        $request = $this->requests->find($token, $requestId);

        return new JsonResponse([
            'status' => (bool)$this->requests->delete($token, $request)
        ]);
    }

    /**
     * @param $tokenId
     * @return JsonResponse
     */
    public function deleteByToken($tokenId)
    {
        $token = $this->tokens->find($tokenId);

        return new JsonResponse([
            'status' => (bool)$this->requests->deleteByToken($token)
        ]);
    }
}
