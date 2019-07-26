from aiohttp import ClientResponse


async def assert_not_404_response(response: ClientResponse) -> None:
    from jsearch.api.error_code import ErrorCode

    assert response.status == 404

    data = await response.json()

    assert data['status']['success'] is False
    assert data['status']['errors'] == [
        {
            'code': ErrorCode.RESOURCE_NOT_FOUND,
            'message': 'Resource not found'
        }
    ]
